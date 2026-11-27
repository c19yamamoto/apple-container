//===----------------------------------------------------------------------===//
// Copyright © 2026 Apple Inc. and the container project authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//===----------------------------------------------------------------------===//

import ContainerAPIClient
import ContainerCommands
import ContainerPersistence
import ContainerResource
import Containerization
import ContainerizationArchive
import ContainerizationError
import ContainerizationExtras
import ContainerizationOCI
import Foundation
import Logging
import NIOCore
import NIOHTTP1

struct DockerEngineRouter: Sendable {
    static let apiVersion = "1.45"
    static let serverVersion = "0.1.0"
    private static let fallbackDNSDomain = "containers.internal"
    private static let hiddenContainerPlugins = [ResourcePluginValues.machine, ResourcePluginValues.cluster]
    private static let maxLogBytesPerStream: UInt64 = 8 * 1024 * 1024
    private static let maxBuildErrorOutputBytes: UInt64 = 64 * 1024

    let containerSystemConfig: ContainerSystemConfig
    let log: Logger
    let runtimeState: DockerEngineRuntimeState

    func handle(method: HTTPMethod, uri: String, headers: HTTPHeaders, body: Data, archiveURL: URL? = nil) async -> DockerEngineResponse {
        guard let components = URLComponents(string: uri) else {
            return Self.error(.badRequest, "不正なリクエスト URI です")
        }
        var pathParts = components.path.split(separator: "/").map(String.init)
        if pathParts.first?.range(of: #"^v\d+\.\d+$"#, options: .regularExpression) != nil {
            pathParts.removeFirst()
        }
        let path = "/" + pathParts.joined(separator: "/")
        let query = Dictionary(components.queryItems?.map { ($0.name, $0.value ?? "") } ?? [], uniquingKeysWith: { _, newest in newest })

        do {
            if (method == .GET || method == .HEAD) && path == "/_ping" {
                return DockerEngineResponse(
                    status: .ok,
                    body: method == .HEAD ? Data() : Data("OK".utf8),
                    contentType: "text/plain"
                )
            }
            if method == .GET && path == "/version" {
                return try Self.json(.ok, [
                    "Version": Self.serverVersion,
                    "ApiVersion": Self.apiVersion,
                    "MinAPIVersion": "1.24",
                    "Os": "linux",
                    "Arch": Platform.current.architecture,
                    "KernelVersion": "Apple Container",
                    "Components": [["Name": "Engine", "Version": Self.serverVersion]],
                ] as [String: Any])
            }
            if method == .GET && path == "/info" {
                return try await info()
            }
            if method == .GET && path == "/containers/json" {
                return try await listContainers(query: query)
            }
            if method == .POST && path == "/build" {
                return try await buildImage(queryItems: components.queryItems ?? [], body: body, archiveURL: archiveURL)
            }
            if method == .POST && path == "/containers/create" {
                return try await createContainer(query: query, body: body)
            }
            if pathParts.count == 3 && pathParts[0] == "containers" {
                let id = pathParts[1]
                if method == .GET && pathParts[2] == "json" {
                    return try await inspectContainer(id: id)
                }
                if method == .POST && pathParts[2] == "start" {
                    let wasRunning = try await startContainer(id: id)
                    return DockerEngineResponse(status: wasRunning ? .notModified : .noContent)
                }
                if method == .POST && pathParts[2] == "stop" {
                    try await stopContainer(id: id, timeout: query["t"])
                    return DockerEngineResponse(status: .noContent)
                }
                if method == .POST && pathParts[2] == "kill" {
                    try await ContainerClient().kill(id: id, signal: query["signal"] ?? "SIGKILL")
                    return DockerEngineResponse(status: .noContent)
                }
                if method == .GET && pathParts[2] == "logs" {
                    return try await containerLogs(id: id, query: query)
                }
                if method == .PUT && pathParts[2] == "archive" {
                    guard let archiveURL else {
                        throw DockerAPIError.badRequest("コンテナへのコピーに archive が必要です")
                    }
                    try await copyArchiveIntoContainer(id: id, query: query, archiveURL: archiveURL)
                    return DockerEngineResponse(status: .ok)
                }
                if method == .POST && pathParts[2] == "exec" {
                    return try await createExec(containerID: id, body: body)
                }
            }
            if pathParts.count == 3 && pathParts[0] == "exec" {
                let id = pathParts[1]
                if method == .POST && pathParts[2] == "start" {
                    return try await startExec(id: id, body: body)
                }
                if method == .GET && pathParts[2] == "json" {
                    return try await inspectExec(id: id)
                }
            }
            if pathParts.count == 2 && pathParts[0] == "containers" {
                let id = pathParts[1]
                if method == .DELETE {
                    await runtimeState.preventRestart(id: id)
                    do {
                        try await ContainerClient().delete(id: id, force: query["force"] == "1" || query["force"] == "true")
                    } catch {
                        await runtimeState.allowRestart(id: id)
                        throw error
                    }
                    await runtimeState.removed(id: id)
                    return DockerEngineResponse(status: .noContent)
                }
            }
            if method == .POST && path == "/images/create" {
                return try await pullImage(query: query, headers: headers)
            }
            if method == .POST && path == "/images/load" {
                return try await loadImages(query: query, archiveURL: archiveURL)
            }
            if method == .GET && path == "/images/json" {
                return try await listImages()
            }
            if method == .GET && path.hasPrefix("/images/") && path.hasSuffix("/json") {
                let imageName = String(path.dropFirst("/images/".count).dropLast("/json".count))
                return try await inspectImage(name: imageName)
            }
            if method == .DELETE && path.hasPrefix("/images/") {
                let reference = String(path.dropFirst("/images/".count))
                try await ClientImage.delete(reference: reference, garbageCollect: true)
                return try Self.json(.ok, [["Untagged": reference]])
            }
            if method == .POST && path == "/networks/create" {
                return try await createNetwork(body: body)
            }
            if method == .GET && path == "/networks" {
                return try await listNetworks()
            }
            if pathParts.count == 2 && pathParts[0] == "networks" {
                if method == .GET {
                    return try await inspectNetwork(id: pathParts[1])
                }
                if method == .DELETE {
                    try await NetworkClient().delete(id: pathParts[1])
                    return DockerEngineResponse(status: .noContent)
                }
            }
            if method == .POST && path == "/volumes/create" {
                return try await createVolume(body: body)
            }
            if method == .GET && path == "/volumes" {
                return try await listVolumes()
            }
            if pathParts.count == 2 && pathParts[0] == "volumes" {
                if method == .GET {
                    return try await inspectVolume(name: pathParts[1])
                }
                if method == .DELETE {
                    try await ClientVolume.delete(name: pathParts[1])
                    return DockerEngineResponse(status: .noContent)
                }
            }
            return Self.error(.notFound, "未対応の Docker Engine API エンドポイントです: \(method) \(path)")
        } catch {
            log.debug("Docker Engine API リクエストに失敗しました", metadata: ["path": "\(path)", "error": "\(error)"])
            let message = (error as? LocalizedError)?.errorDescription ?? String(describing: error)
            return Self.error(Self.status(for: error), message)
        }
    }

    private func info() async throws -> DockerEngineResponse {
        let containers = try await ContainerClient().list(filters: .all.withoutPlugins(Self.hiddenContainerPlugins))
        let images = try await ClientImage.list()
        let networks = try await NetworkClient().list()
        let volumes = try await ClientVolume.list()
        return try Self.json(.ok, [
            "ID": "apple-container",
            "Containers": containers.count,
            "ContainersRunning": containers.filter { $0.status == .running }.count,
            "ContainersPaused": 0,
            "ContainersStopped": containers.filter { $0.status != .running }.count,
            "Images": images.count,
            "Driver": "containerization",
            "DriverStatus": [],
            "DockerRootDir": "",
            "Name": "container",
            "Labels": [],
            "OperatingSystem": "Apple Container",
            "OSType": "linux",
            "Architecture": Platform.current.architecture,
            "NCPU": ProcessInfo.processInfo.processorCount,
            "MemTotal": ProcessInfo.processInfo.physicalMemory,
            "ServerVersion": Self.serverVersion,
            "SecurityOptions": [],
            "Warnings": [],
            "Swarm": ["LocalNodeState": "inactive"],
            "DefaultRuntime": "container-runtime-linux",
            "Runtimes": ["container-runtime-linux": ["path": "container-runtime-linux"]],
            "Networks": networks.count,
            "Volumes": volumes.count,
        ] as [String: Any])
    }

    private func listContainers(query: [String: String]) async throws -> DockerEngineResponse {
        let all = query["all"] == "1" || query["all"] == "true"
        let filters = try Self.containerFilters(query["filters"]).withoutPlugins(Self.hiddenContainerPlugins)
        var snapshots = try await ContainerClient().list(filters: filters)
        if !all {
            snapshots = snapshots.filter { $0.status == .running }
        }
        let summaries = await snapshots.asyncMap { snapshot in
            let imageID = (try? await ClientImage(description: snapshot.configuration.image).manifest(for: .current).config.digest)
                ?? snapshot.configuration.image.digest
            return Self.containerSummary(
                snapshot,
                health: await runtimeState.health(id: snapshot.id),
                restartPolicy: await runtimeState.restartPolicy(id: snapshot.id),
                restartCount: await runtimeState.restartCount(id: snapshot.id),
                privileged: await runtimeState.isPrivileged(id: snapshot.id),
                imageID: imageID
            )
        }
        return try Self.json(.ok, summaries)
    }

    private func buildImage(queryItems: [URLQueryItem], body: Data, archiveURL: URL?) async throws -> DockerEngineResponse {
        guard archiveURL != nil || !body.isEmpty else {
            throw DockerAPIError.badRequest("Docker build context が空です")
        }
        let query = Dictionary(queryItems.compactMap { item in item.value.map { (item.name, $0) } }, uniquingKeysWith: { _, newest in newest })
        let supportedBuildOptions: Set<String> = [
            "dockerfile", "t", "buildargs", "labels", "target", "nocache", "pull", "q", "forcerm", "rm", "platform", "ssh", "version",
            "remote", "cachefrom", "memory", "memswap", "cpushares", "cpusetcpus", "cpuperiod", "cpuquota", "shmsize", "squash", "networkmode", "extrahosts",
        ]
        for item in queryItems where !supportedBuildOptions.contains(item.name) && !(item.value ?? "").isEmpty {
            throw DockerAPIError.unsupported("build option \(item.name) はサポートされていません")
        }
        let unsupportedBuildOptionDefaults: [String: Set<String>] = [
            "remote": [""],
            "cachefrom": ["", "[]"],
            "memory": ["", "0"],
            "memswap": ["", "0"],
            "cpushares": ["", "0"],
            "cpusetcpus": [""],
            "cpuperiod": ["", "0"],
            "cpuquota": ["", "0"],
            "shmsize": ["", "0", "67108864"],
            "squash": ["", "0", "false"],
            "networkmode": ["", "default"],
            "extrahosts": ["", "[]"],
        ]
        for item in queryItems {
            if let acceptedValues = unsupportedBuildOptionDefaults[item.name], !acceptedValues.contains(item.value ?? "") {
                throw DockerAPIError.unsupported("build option \(item.name) は指定された値ではサポートされていません")
            }
        }
        for flag in ["nocache", "pull", "q", "forcerm", "rm"] {
            _ = try Self.queryFlag(query[flag], field: flag)
        }
        if let version = query["version"], !["1", "2"].contains(version) {
            throw DockerAPIError.unsupported("build version \(version) はサポートされていません")
        }
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("docker-build-\(UUID().uuidString)", isDirectory: true)
        let context = root.appendingPathComponent("context", isDirectory: true)
        try FileManager.default.createDirectory(at: context, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let archive: URL
        if let archiveURL {
            archive = archiveURL
        } else {
            archive = root.appendingPathComponent("context.tar")
            try body.write(to: archive, options: .atomic)
        }

        do {
            let reader = try ArchiveReader(file: archive)
            var uncompressedBytes: Int64 = 0
            var entryCount = 0
            for (entry, _) in reader.makeStreamingIterator() {
                entryCount += 1
                guard entryCount <= 100_000 else {
                    throw DockerAPIError.badRequest("build context のファイル数は 100,000 以下である必要があります")
                }
                guard entry.fileType == .regular else { continue }
                let entrySize = max(entry.size ?? 0, 0)
                let remainingBytes = 1_073_741_824 - uncompressedBytes
                guard entrySize <= remainingBytes else {
                    throw DockerAPIError.badRequest("展開後の build context は 1 GiB 以下である必要があります")
                }
                uncompressedBytes += entrySize
            }
            try reader.throwIfStreamFailed()
        } catch let error as DockerAPIError {
            throw error
        } catch {
            throw DockerAPIError.badRequest("build context を読み込めません: \(error)")
        }

        let rejectedMembers: [String]
        do {
            rejectedMembers = try ArchiveReader(file: archive).extractContents(to: context)
        } catch {
            throw DockerAPIError.badRequest("build context を展開できません: \(error)")
        }
        guard rejectedMembers.isEmpty else {
            throw DockerAPIError.badRequest("build context に不正なパスが含まれています")
        }
        let dockerfile = query["dockerfile"] ?? "Dockerfile"
        let dockerfileURL = URL(fileURLWithPath: dockerfile, relativeTo: context).standardizedFileURL
        let contextPath = context.standardizedFileURL.path
        let dockerfileType = try? FileManager.default.attributesOfItem(atPath: dockerfileURL.path)[.type] as? FileAttributeType
        guard !dockerfile.hasPrefix("/"), dockerfileURL.path.hasPrefix(contextPath + "/"),
            dockerfileType == .typeRegular
        else {
            throw DockerAPIError.badRequest("Dockerfile が build context 内にありません")
        }

        var options = BuildOptions()
        options.contextDir = contextPath
        options.file = dockerfileURL.path
        options.buildArg = try Self.buildOptions(query["buildargs"], field: "buildargs")
            .map { "\($0.key)=\($0.value)" }
        options.label = try Self.buildOptions(query["labels"], field: "labels")
            .map { "\($0.key)=\($0.value)" }
        options.target = query["target"] ?? ""
        options.noCache = try Self.queryFlag(query["nocache"], field: "nocache")
        options.pull = try Self.queryFlag(query["pull"], field: "pull")
        if let platform = query["platform"], !platform.isEmpty {
            options.platform = [[platform]]
            do {
                _ = try options.resolvePlatforms()
            } catch {
                throw DockerAPIError.badRequest("build platform が不正です: \(error)")
            }
        }
        if let ssh = query["ssh"], !ssh.isEmpty {
            options.ssh = ssh
        }
        options.progress = .plain
        options.output = ["type=oci"]
        let tags = queryItems.filter { $0.name == "t" }.compactMap(\.value)
        let resultTag = tags.first ?? "apple-container-build:\(UUID().uuidString.lowercased())"
        options.targetImageNames = tags.isEmpty ? [resultTag] : tags
        let buildTags = options.targetImageNames
        let buildArgs = options.buildArg
        let buildLabels = options.label
        let buildTarget = options.target
        let buildNoCache = options.noCache
        let buildPull = options.pull
        let buildSSH = query["ssh"] ?? ""
        let buildPlatform = query["platform"]
        try await Task.detached(priority: .userInitiated) {
            try await self.runBuildProcess(
                contextPath: contextPath,
                dockerfilePath: dockerfileURL.path,
                tags: buildTags,
                buildArgs: buildArgs,
                labels: buildLabels,
                target: buildTarget,
                platform: buildPlatform,
                noCache: buildNoCache,
                pull: buildPull,
                ssh: buildSSH
            )
        }.value

        let image = try await ClientImage.get(reference: resultTag, containerSystemConfig: containerSystemConfig)
        let imageID = Self.dockerImageID(try await image.manifest(for: .current).config.digest)
        let shortImageID = String(imageID.dropFirst("sha256:".count).prefix(12))
        var response = try JSONSerialization.data(withJSONObject: ["stream": "Successfully built \(shortImageID)\n"])
        response.append(0x0A)
        response.append(try JSONSerialization.data(withJSONObject: ["aux": ["ID": imageID]]))
        response.append(0x0A)
        return DockerEngineResponse(status: .ok, body: response)
    }

    private func runBuildProcess(
        contextPath: String,
        dockerfilePath: String,
        tags: [String],
        buildArgs: [String],
        labels: [String],
        target: String,
        platform: String?,
        noCache: Bool,
        pull: Bool,
        ssh: String
    ) async throws {
        let installRoot = ProcessInfo.processInfo.environment["CONTAINER_INSTALL_ROOT"]
            ?? URL(fileURLWithPath: CommandLine.arguments[0])
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .path
        let executableURL = URL(fileURLWithPath: installRoot).appendingPathComponent("bin/container")
        guard FileManager.default.isExecutableFile(atPath: executableURL.path) else {
            throw DockerAPIError.internalError("container build executable が見つかりません: \(executableURL.path)")
        }

        var arguments = ["build", "--progress", "plain", "--output", "type=oci"]
        arguments.append(contentsOf: tags.flatMap { ["--tag", $0] })
        arguments.append(contentsOf: buildArgs.flatMap { ["--build-arg", $0] })
        arguments.append(contentsOf: labels.flatMap { ["--label", $0] })
        if !target.isEmpty {
            arguments.append(contentsOf: ["--target", target])
        }
        if let platform, !platform.isEmpty {
            arguments.append(contentsOf: ["--platform", platform])
        }
        if noCache {
            arguments.append("--no-cache")
        }
        if pull {
            arguments.append("--pull")
        }
        if !ssh.isEmpty {
            arguments.append(contentsOf: ["--ssh", ssh])
        }
        arguments.append(contentsOf: ["--file", dockerfilePath, contextPath])

        let outputRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("docker-build-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: outputRoot, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: outputRoot) }
        let stdoutURL = outputRoot.appendingPathComponent("stdout.log")
        let stderrURL = outputRoot.appendingPathComponent("stderr.log")
        guard FileManager.default.createFile(atPath: stdoutURL.path, contents: nil),
            FileManager.default.createFile(atPath: stderrURL.path, contents: nil)
        else {
            throw DockerAPIError.internalError("build の出力ファイルを作成できません")
        }

        let stdout = try FileHandle(forWritingTo: stdoutURL)
        let stderr = try FileHandle(forWritingTo: stderrURL)
        defer {
            try? stdout.close()
            try? stderr.close()
        }

        let process = Process()
        process.executableURL = executableURL
        process.arguments = arguments
        process.currentDirectoryURL = URL(fileURLWithPath: contextPath)
        process.standardOutput = stdout
        process.standardError = stderr
        var environment = ProcessInfo.processInfo.environment
        environment.removeValue(forKey: "XPC_SERVICE_NAME")
        process.environment = environment

        let status: Int32 = try await withCheckedThrowingContinuation { continuation in
            process.terminationHandler = { process in
                continuation.resume(returning: process.terminationStatus)
            }
            do {
                try process.run()
            } catch {
                continuation.resume(throwing: error)
            }
        }
        try stdout.close()
        try stderr.close()

        guard status == 0 else {
            let errorOutput = Self.buildErrorOutput(at: stderrURL)
            let message = errorOutput.flatMap { $0.isEmpty ? nil : $0 }
                ?? "container build が終了コード \(status) で失敗しました"
            throw DockerAPIError.internalError(message)
        }
    }

    private static func buildErrorOutput(at url: URL) -> String? {
        guard let handle = try? FileHandle(forReadingFrom: url) else { return nil }
        defer { try? handle.close() }
        guard let endOffset = try? handle.seekToEnd() else { return nil }
        let startOffset = endOffset > maxBuildErrorOutputBytes ? endOffset - maxBuildErrorOutputBytes : 0
        guard (try? handle.seek(toOffset: startOffset)) != nil,
            let data = try? handle.readToEnd()
        else {
            return nil
        }
        let output = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
        guard !output.isEmpty else { return nil }
        return startOffset > 0 ? "build log の末尾のみ表示しています。\n\(output)" : output
    }

    private func loadImages(query: [String: String], archiveURL: URL?) async throws -> DockerEngineResponse {
        guard let archiveURL else {
            throw DockerAPIError.badRequest("POST /images/load には tar archive が必要です")
        }
        for (name, value) in query where name != "quiet" && !value.isEmpty {
            throw DockerAPIError.unsupported("image load option \(name) はサポートされていません")
        }
        let quiet = try Self.queryFlag(query["quiet"], field: "quiet")
        let result = try await ClientImage.load(from: archiveURL.path, force: false)
        for image in result.images {
            try await image.unpack(platform: nil)
        }
        guard !result.images.isEmpty else {
            throw DockerAPIError.badRequest("tar archive に読み込めるイメージがありません")
        }
        guard !quiet else {
            return DockerEngineResponse(status: .ok)
        }
        let lines = try result.images.map { image in
            try JSONSerialization.data(withJSONObject: ["stream": "Loaded image: \(image.reference)\n"])
        }
        var body = Data()
        for line in lines {
            body.append(line)
            body.append(0x0A)
        }
        return DockerEngineResponse(status: .ok, body: body)
    }

    private static func buildOptions(_ value: String?, field: String) throws -> [String: String] {
        guard let value, !value.isEmpty else { return [:] }
        do {
            return try JSONDecoder().decode([String: String].self, from: Data(value.utf8))
        } catch {
            throw DockerAPIError.badRequest("\(field) は文字列値を持つ JSON object である必要があります")
        }
    }

    private static func queryFlag(_ value: String?, field: String) throws -> Bool {
        guard let value else { return false }
        switch value.lowercased() {
        case "1", "true": return true
        case "0", "false", "": return false
        default: throw DockerAPIError.badRequest("build option \(field) は boolean である必要があります")
        }
    }

    private func createContainer(query: [String: String], body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerCreateRequest.self, from: body)
        try Self.validateUnsupportedCreateConfig(body: body)
        if let healthcheck = request.healthcheck, !healthcheck.isDisabled, healthcheck.command == nil {
            throw DockerAPIError.badRequest("Healthcheck.Test に CMD または CMD-SHELL のコマンドが必要です")
        }
        if let healthcheck = request.healthcheck,
            [healthcheck.interval, healthcheck.timeout, healthcheck.startPeriod, healthcheck.startInterval].compactMap({ $0 }).contains(where: { $0 < 0 })
                || (healthcheck.retries ?? 0) < 0
        {
            throw DockerAPIError.badRequest("Healthcheck の期間と Retries は 0 以上である必要があります")
        }
        if request.healthcheck?.isDisabled == true, request.healthcheck?.test?.count != 1 {
            throw DockerAPIError.badRequest("Healthcheck.Test の NONE には他の値を指定できません")
        }
        let restartPolicy = request.hostConfig?.restartPolicy ?? DockerRestartPolicy()
        guard ["no", "always", "unless-stopped", "on-failure"].contains(restartPolicy.normalizedName) else {
            throw DockerAPIError.badRequest("HostConfig.RestartPolicy.Name が不正です")
        }
        guard restartPolicy.maximumRetryCount >= 0 else {
            throw DockerAPIError.badRequest("HostConfig.RestartPolicy.MaximumRetryCount は 0 以上である必要があります")
        }
        if restartPolicy.normalizedName != "on-failure", restartPolicy.maximumRetryCount != 0 {
            throw DockerAPIError.badRequest("MaximumRetryCount は on-failure の場合のみ指定できます")
        }
        guard !request.image.isEmpty else {
            throw DockerAPIError.badRequest("コンテナ作成時には Image が必要です")
        }
        let id = query["name"].flatMap { $0.isEmpty ? nil : $0 } ?? UUID().uuidString.lowercased()
        let image = try await ClientImage.get(reference: request.image, containerSystemConfig: containerSystemConfig)
        let imageConfig = try await image.config(for: .current).config
        _ = try await image.getCreateSnapshot(platform: .current)

        let command = (request.entrypoint ?? imageConfig?.entrypoint ?? []) + (request.cmd ?? imageConfig?.cmd ?? [])
        guard let executable = command.first else {
            throw DockerAPIError.badRequest("イメージにコマンドがなく、コンテナ作成時にも指定されていません")
        }
        let labels = request.labels ?? [:]
        let requestedUser = request.user.flatMap { $0.isEmpty ? nil : $0 }
        let requestedWorkingDirectory = request.workingDir.flatMap { $0.isEmpty ? nil : $0 }
        let processUser = Parser.user(user: requestedUser ?? imageConfig?.user, uid: nil, gid: nil)
        let process = ProcessConfiguration(
            executable: executable,
            arguments: Array(command.dropFirst()),
            environment: try Parser.allEnv(imageEnvs: imageConfig?.env ?? [], envFiles: [], envs: request.env ?? []),
            workingDirectory: requestedWorkingDirectory ?? imageConfig?.workingDir ?? "/",
            terminal: request.tty ?? false,
            user: processUser.user,
            supplementalGroups: processUser.groups
        )

        let config = try await makeContainerConfiguration(
            id: id,
            request: request,
            image: image,
            process: process,
            labels: labels,
            stopSignal: request.stopSignal.flatMap { $0.isEmpty ? nil : $0 } ?? imageConfig?.stopSignal
        )
        let kernel = try await ClientKernel.getDefaultKernel(for: .current)
        try await ContainerClient().create(configuration: config, options: .default, kernel: kernel)
        await runtimeState.configureHealthcheck(id: id, healthcheck: request.healthcheck, process: process)
        await runtimeState.configureRestartPolicy(id: id, policy: restartPolicy, privileged: request.hostConfig?.privileged ?? false)
        return try Self.json(.created, ["Id": id, "Warnings": []] as [String: Any])
    }

    private func makeContainerConfiguration(
        id: String,
        request: DockerCreateRequest,
        image: ClientImage,
        process: ProcessConfiguration,
        labels: [String: String],
        stopSignal: String?
    ) async throws -> ContainerConfiguration {
        let host = request.hostConfig ?? DockerHostConfig()
        var config = ContainerConfiguration(id: id, image: image.description, process: process)
        config.runtimeHandler = "container-runtime-linux"
        config.labels = labels
        config.stopSignal = stopSignal
        config.useInit = host.useInit ?? false
        config.resources = try Parser.resources(
            cpus: nil,
            memory: nil,
            defaultCPUs: containerSystemConfig.container.cpus,
            defaultMemory: containerSystemConfig.container.memory
        )
        config.readOnly = host.readonlyRootfs ?? false
        if host.privileged == true {
            config.capAdd = ["ALL"]
            config.maskedPaths = []
            config.readonlyPaths = []
        } else {
            let capabilities = try Parser.capabilities(capAdd: host.capAdd ?? [], capDrop: host.capDrop ?? [])
            config.capAdd = capabilities.capAdd
            config.capDrop = capabilities.capDrop
        }
        if let memory = host.memory, memory > 0 {
            config.resources.memoryInBytes = UInt64(memory)
        } else if let memory = host.memory, memory < 0 {
            throw DockerAPIError.badRequest("HostConfig.Memory は 0 以上である必要があります")
        }
        if let nanoCPUs = host.nanoCPUs, nanoCPUs > 0 {
            guard nanoCPUs.isMultiple(of: 1_000_000_000) else {
                throw DockerAPIError.unsupported("小数単位の CPU 制限はサポートされていません")
            }
            config.resources.cpus = Int(nanoCPUs / 1_000_000_000)
        } else if let nanoCPUs = host.nanoCPUs, nanoCPUs < 0 {
            throw DockerAPIError.badRequest("HostConfig.NanoCpus は 0 以上である必要があります")
        }
        if let size = host.shmSize, size > 0 {
            config.shmSize = UInt64(size)
        } else if let size = host.shmSize, size < 0 {
            throw DockerAPIError.badRequest("HostConfig.ShmSize は 0 以上である必要があります")
        }
        config.publishedPorts = try Self.publishPorts(host.portBindings ?? [:])
        config.mounts = try await makeMounts(host: host)
        config.networks = try await makeNetworkAttachments(id: id, request: request, host: host)
        let defaultDNS = containerSystemConfig.container.dns
        let dnsDomain = Self.dockerDNSDomain(containerSystemConfig)
        var searchDomains = host.dnsSearch.flatMap { $0.isEmpty ? nil : $0 } ?? defaultDNS?.searchDomains ?? []
        let networkSearchDomains = config.networks
            .map { Self.networkDNSDomain(network: $0.network, baseDomain: dnsDomain) }
            .sorted()
        for searchDomain in networkSearchDomains + [dnsDomain] {
            if !searchDomains.contains(where: { AttachmentOptions.canonicalHostname($0) == AttachmentOptions.canonicalHostname(searchDomain) }) {
                searchDomains.append(searchDomain)
            }
        }
        config.dns = .init(
            nameservers: host.dns.flatMap { $0.isEmpty ? nil : $0 } ?? defaultDNS?.nameservers ?? [],
            domain: dnsDomain,
            searchDomains: searchDomains,
            options: host.dnsOptions.flatMap { $0.isEmpty ? nil : $0 } ?? defaultDNS?.options ?? []
        )
        if let sysctls = host.sysctls {
            config.sysctls = sysctls
        }
        return config
    }

    private func makeNetworkAttachments(id: String, request: DockerCreateRequest, host: DockerHostConfig) async throws -> [AttachmentConfiguration] {
        let networkClient = NetworkClient()
        let networkMode = host.networkMode?.lowercased()
        if networkMode == "none" {
            return []
        }
        guard networkMode != "host" else {
            throw DockerAPIError.unsupported("host network mode is not supported")
        }

        let endpoints = request.networkingConfig?.endpointsConfig ?? [:]
        var entries: [(String, DockerEndpointConfig)] = Array(endpoints)
        if entries.isEmpty {
            let requestedName = networkMode.flatMap { ["", "default", "bridge"].contains($0) ? nil : $0 }
            let network: NetworkResource?
            if let requestedName {
                network = try await networkClient.get(id: requestedName)
            } else {
                network = try await networkClient.builtin
            }
            guard let network else {
                throw DockerAPIError.internalError("既定ネットワークがありません")
            }
            entries = [(network.id, DockerEndpointConfig())]
        }

        let dnsDomain = Self.dockerDNSDomain(containerSystemConfig)
        let hostname = request.hostname.flatMap { $0.isEmpty ? nil : $0 } ?? id
        let qualifiedHostname = Self.qualify(hostname, domain: dnsDomain)
        return try await entries.asyncMap { entry in
            let (networkName, endpoint) = entry
            let network = try await networkClient.get(id: networkName)
            let aliases = (endpoint.aliases ?? [])
                .map { Self.qualifyNetworkAlias($0, network: network.id, baseDomain: dnsDomain) }
                .filter {
                    AttachmentOptions.canonicalHostname($0) != AttachmentOptions.canonicalHostname(qualifiedHostname)
                        && AttachmentOptions.canonicalHostname($0) != AttachmentOptions.canonicalHostname(hostname)
                }
                .reduce(into: [String]()) { result, alias in
                    if !result.contains(where: { AttachmentOptions.canonicalHostname($0) == AttachmentOptions.canonicalHostname(alias) }) {
                        result.append(alias)
                    }
                }
            return AttachmentConfiguration(
                network: network.id,
                options: AttachmentOptions(hostname: qualifiedHostname, aliases: aliases)
            )
        }
    }

    private func makeMounts(host: DockerHostConfig) async throws -> [Filesystem] {
        var mounts = [Filesystem]()
        for mount in host.mounts ?? [] {
            guard let target = mount.target, !target.isEmpty else {
                throw DockerAPIError.badRequest("Mounts の Target が必要です")
            }
            let readOnly = mount.readOnly ?? false
            let options = readOnly ? ["ro"] : []
            switch mount.type?.lowercased() {
            case "bind":
                guard let source = mount.source, !source.isEmpty else {
                    throw DockerAPIError.badRequest("bind mount の Source が必要です")
                }
                mounts.append(.virtiofs(source: source, destination: target, options: options))
            case "tmpfs":
                mounts.append(.tmpfs(destination: target, options: options))
            case "volume":
                let name = mount.source.flatMap { $0.isEmpty ? nil : $0 } ?? UUID().uuidString.lowercased()
                let volume = try await Self.getOrCreateVolume(name: name)
                mounts.append(.volume(name: name, format: volume.format, source: volume.source, destination: target, options: options))
            case .none:
                throw DockerAPIError.badRequest("Mounts の Type が必要です")
            default:
                throw DockerAPIError.unsupported("指定された mount type はサポートされていません")
            }
        }
        for bind in host.binds ?? [] {
            let parts = bind.split(separator: ":", maxSplits: 2, omittingEmptySubsequences: false).map(String.init)
            guard parts.count >= 2, !parts[0].isEmpty, !parts[1].isEmpty else {
                throw DockerAPIError.badRequest("Binds の形式が正しくありません: \(bind)")
            }
            let bindOptions = parts.count == 3 ? parts[2].split(separator: ",").map(String.init) : []
            guard bindOptions.allSatisfy({ ["ro", "rw"].contains($0) }) else {
                throw DockerAPIError.unsupported("bind mount の mode オプションは ro と rw のみサポートされています")
            }
            guard !(bindOptions.contains("ro") && bindOptions.contains("rw")) else {
                throw DockerAPIError.badRequest("bind mount の mode に ro と rw は同時指定できません")
            }
            let options = bindOptions.contains("ro") ? ["ro"] : []
            if parts[0].hasPrefix("/") {
                mounts.append(.virtiofs(source: parts[0], destination: parts[1], options: options))
            } else {
                let volume = try await Self.getOrCreateVolume(name: parts[0])
                mounts.append(.volume(name: parts[0], format: volume.format, source: volume.source, destination: parts[1], options: options))
            }
        }
        for (target, value) in host.tmpfs ?? [:] {
            let tmpfsOptions = value.split(separator: ",").map(String.init)
            guard tmpfsOptions.allSatisfy({ ["ro", "rw"].contains($0) }) else {
                throw DockerAPIError.unsupported("tmpfs のサイズや mode オプションはサポートされていません")
            }
            guard !(tmpfsOptions.contains("ro") && tmpfsOptions.contains("rw")) else {
                throw DockerAPIError.badRequest("tmpfs の mode に ro と rw は同時指定できません")
            }
            mounts.append(.tmpfs(destination: target, options: tmpfsOptions.contains("ro") ? ["ro"] : []))
        }
        return mounts
    }

    private static func getOrCreateVolume(name: String) async throws -> VolumeConfiguration {
        if let volume = try? await ClientVolume.inspect(name) {
            return volume
        }
        return try await ClientVolume.create(name: name)
    }

    private static func publishPorts(_ bindings: [String: [DockerPortBinding]?]) throws -> [PublishPort] {
        var values = [String]()
        for (containerPort, entries) in bindings {
            guard let entries else { continue }
            let parts = containerPort.split(separator: "/", maxSplits: 1).map(String.init)
            let port = parts.first ?? containerPort
            let proto = parts.count > 1 ? parts[1] : "tcp"
            for binding in entries {
                guard let hostPort = binding.hostPort, !hostPort.isEmpty else {
                    throw DockerAPIError.unsupported("自動割り当ての host port はサポートされていません")
                }
                let hostIP = binding.hostIP ?? ""
                let formattedIP = hostIP.contains(":") ? "[\(hostIP)]" : hostIP
                values.append("\(formattedIP.isEmpty ? "" : "\(formattedIP):")\(hostPort):\(port)/\(proto)")
            }
        }
        return try Parser.publishPorts(values)
    }

    private func startContainer(id: String) async throws -> Bool {
        if try await ContainerClient().get(id: id).status == .running {
            return true
        }
        await runtimeState.preventRestart(id: id)
        do {
            let process = try await ContainerClient().bootstrap(id: id, stdio: [nil, nil, nil])
            try await process.start()
            await runtimeState.started(id: id)
            Task {
                await monitorContainerExit(id: id, process: process)
            }
            return false
        } catch {
            await runtimeState.allowRestart(id: id)
            throw error
        }
    }

    private func monitorContainerExit(id: String, process: ClientProcess) async {
        var process = process
        while let exitCode = try? await process.wait() {
            guard let plan = await runtimeState.exited(id: id, exitCode: exitCode) else { return }
            while true {
                do {
                    try await Task.sleep(for: plan.delay)
                    guard await runtimeState.beginRestart(id: id, generation: plan.generation) else { return }
                    let nextProcess = try await ContainerClient().bootstrap(id: id, stdio: [nil, nil, nil])
                    guard await runtimeState.restartIsCurrent(id: id, generation: plan.generation) else { return }
                    try await nextProcess.start()
                    await runtimeState.started(id: id, automaticRestart: true)
                    process = nextProcess
                    break
                } catch {
                    await runtimeState.restartFailed(id: id, generation: plan.generation)
                    if Task.isCancelled { return }
                }
            }
        }
    }

    private func stopContainer(id: String, timeout: String?) async throws {
        let seconds = timeout.flatMap(Int32.init) ?? ContainerStopOptions.default.timeoutInSeconds
        await runtimeState.preventRestart(id: id)
        do {
            try await ContainerClient().stop(id: id, opts: ContainerStopOptions(timeoutInSeconds: seconds, signal: nil))
        } catch {
            await runtimeState.allowRestart(id: id)
            throw error
        }
    }

    private func inspectContainer(id: String) async throws -> DockerEngineResponse {
        let snapshot = try await ContainerClient().get(id: id)
        let runtime = await runtimeState.state(id: id)
        let healthcheck = await runtimeState.healthcheck(id: id)
        let health = await runtimeState.health(id: id)
        let restartPolicy = await runtimeState.restartPolicy(id: id)
        let restartCount = await runtimeState.restartCount(id: id)
        let privileged = await runtimeState.isPrivileged(id: id)
        let imageID = Self.dockerImageID(try await ClientImage(description: snapshot.configuration.image).manifest(for: .current).config.digest)
        return try Self.json(
            .ok,
            Self.containerInspection(snapshot, runtime: runtime, healthcheck: healthcheck, health: health, restartPolicy: restartPolicy, restartCount: restartCount, privileged: privileged, imageID: imageID)
        )
    }

    private func containerLogs(id: String, query: [String: String]) async throws -> DockerEngineResponse {
        let snapshot = try await ContainerClient().get(id: id)
        let handles = try await ContainerClient().logs(id: id)
        let logs = try handles.enumerated().map { index, handle in
            defer { try? handle.close() }
            let stream = index == 0 ? UInt8(1) : UInt8(2)
            let enabled = query[index == 0 ? "stdout" : "stderr"] == "1" || query[index == 0 ? "stdout" : "stderr"] == "true"
            guard enabled else { return (stream, Data()) }
            let endOffset = try handle.seekToEnd()
            let startOffset = endOffset > Self.maxLogBytesPerStream ? endOffset - Self.maxLogBytesPerStream : 0
            try handle.seek(toOffset: startOffset)
            return (stream, try handle.readToEnd() ?? Data())
        }
        var body = Data()
        if snapshot.configuration.initProcess.terminal {
            logs.forEach { body.append($0.1) }
        } else {
            for (stream, data) in logs where !data.isEmpty {
                body.append(contentsOf: [stream, 0, 0, 0])
                let length = UInt32(data.count)
                body.append(contentsOf: [UInt8(length >> 24), UInt8(truncatingIfNeeded: length >> 16), UInt8(truncatingIfNeeded: length >> 8), UInt8(truncatingIfNeeded: length)])
                body.append(data)
            }
        }
        return DockerEngineResponse(status: .ok, body: body, contentType: "application/vnd.docker.raw-stream")
    }

    private func copyArchiveIntoContainer(id: String, query: [String: String], archiveURL: URL) async throws {
        guard let requestedPath = query["path"], requestedPath.hasPrefix("/"), !requestedPath.contains("\0") else {
            throw DockerAPIError.badRequest("PUT /containers/{id}/archive には絶対 path が必要です")
        }

        let destinationPath = URL(fileURLWithPath: requestedPath).standardizedFileURL.path
        let destinationURL = URL(fileURLWithPath: destinationPath, isDirectory: true)
        let destinationName = destinationPath == "/" ? "root" : destinationURL.lastPathComponent
        let stagingRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("docker-engine-copyin-\(UUID().uuidString)", isDirectory: true)
        let archiveContents = stagingRoot.appendingPathComponent(destinationName, isDirectory: true)
        try FileManager.default.createDirectory(
            at: archiveContents,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        defer { try? FileManager.default.removeItem(at: stagingRoot) }

        let rejectedPaths: [String]
        do {
            rejectedPaths = try ArchiveReader(file: archiveURL).extractContents(to: archiveContents)
        } catch ContainerizationArchive.ArchiveError.failedToExtractArchive(let reason)
            where reason == "no entries found in archive"
        {
            // 空の copy-in は no-op とするが、存在しない ID は通常どおり拒否する。
            _ = try await ContainerClient().get(id: id)
            return
        }
        let rootDirectoryMarkers: Set<String> = ["", ".", "./", "/"]
        let unsafePaths = rejectedPaths.filter { !rootDirectoryMarkers.contains($0) }
        guard unsafePaths.isEmpty else {
            throw DockerAPIError.badRequest("archive にコピー先から逸脱するパスがあります: \(unsafePaths.joined(separator: ", "))")
        }

        let client = ContainerClient()
        if destinationPath != "/" {
            let destinationParent = destinationURL.deletingLastPathComponent().path
            try await runContainerCommand(id: id, executable: "/bin/mkdir", arguments: ["-p", "--", destinationParent])
            try await client.copyIn(
                id: id,
                source: archiveContents.path,
                destination: destinationParent,
                createParents: true
            )
            return
        }

        let entries = try FileManager.default.contentsOfDirectory(
            at: archiveContents,
            includingPropertiesForKeys: [.isDirectoryKey, .isSymbolicLinkKey],
            options: []
        )
        for entry in entries {
            let attributes = try FileManager.default.attributesOfItem(atPath: entry.path)
            let type = attributes[.type] as? FileAttributeType
            if type == .typeDirectory {
                try await client.copyIn(id: id, source: entry.path, destination: "/", createParents: true)
            } else if type == .typeSymbolicLink {
                let target = try FileManager.default.destinationOfSymbolicLink(atPath: entry.path)
                try await runContainerCommand(id: id, executable: "/bin/ln", arguments: ["-s", "--", target, "/\(entry.lastPathComponent)"])
            } else {
                let mode = (attributes[.posixPermissions] as? NSNumber)?.uint32Value ?? 0o644
                try await client.copyIn(id: id, source: entry.path, destination: "/", mode: mode, createParents: true)
            }
        }
    }

    private func runContainerCommand(id: String, executable: String, arguments: [String]) async throws {
        let client = ContainerClient()
        let container = try await client.get(id: id)
        var configuration = container.configuration.initProcess
        configuration.executable = executable
        configuration.arguments = arguments
        configuration.terminal = false
        let process = try await client.createProcess(
            containerId: id,
            processId: UUID().uuidString.lowercased(),
            configuration: configuration,
            stdio: [nil, nil, nil]
        )
        try await process.start()
        let exitCode = try await process.wait()
        guard exitCode == 0 else {
            throw DockerAPIError.badRequest("コンテナ内コマンドが終了コード \(exitCode) で失敗しました")
        }
    }

    private func createExec(containerID: String, body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerExecCreateRequest.self, from: body)
        guard let command = request.command, let executable = command.first, !executable.isEmpty else {
            throw DockerAPIError.badRequest("Exec.Cmd にコマンドが必要です")
        }
        let client = ContainerClient()
        let container = try await client.get(id: containerID)
        guard container.status == .running else {
            throw DockerAPIError.conflict("実行中のコンテナでのみ exec を作成できます")
        }

        var process = container.configuration.initProcess
        process.executable = executable
        process.arguments = Array(command.dropFirst())
        process.environment.append(contentsOf: try Parser.allEnv(imageEnvs: [], envFiles: [], envs: request.environment ?? []))
        if let workingDirectory = request.workingDirectory, !workingDirectory.isEmpty {
            process.workingDirectory = workingDirectory
        }
        process.terminal = request.tty ?? false
        let processUser = Parser.user(user: request.user, uid: nil, gid: nil, defaultUser: process.user)
        process.user = processUser.user
        process.supplementalGroups.append(contentsOf: processUser.groups)

        let id = await runtimeState.createExec(
            containerID: containerID,
            process: process,
            tty: request.tty ?? false,
            attachStdin: request.attachStdin ?? false,
            attachStdout: request.attachStdout ?? false,
            attachStderr: request.attachStderr ?? false
        )
        return try Self.json(.created, ["Id": id, "Warnings": []] as [String: Any])
    }

    private func startExec(id: String, body: Data) async throws -> DockerEngineResponse {
        let request = body.isEmpty ? DockerExecStartRequest() : try JSONDecoder().decode(DockerExecStartRequest.self, from: body)
        guard let exec = await runtimeState.beginExec(id: id) else {
            throw DockerAPIError.notFound("exec インスタンスが見つからないか、既に開始されています")
        }
        if request.detach == true {
            do {
                let stdin = exec.attachStdin ? Pipe() : nil
                let process = try await ContainerClient().createProcess(
                    containerId: exec.containerID,
                    processId: UUID().uuidString.lowercased(),
                    configuration: exec.process,
                    stdio: [stdin?.fileHandleForReading, nil, nil]
                )
                try await process.start()
                try? stdin?.fileHandleForWriting.close()
                Task.detached { [runtimeState] in
                    let exitCode = (try? await process.wait()) ?? 127
                    await runtimeState.completeExec(id: id, exitCode: exitCode)
                }
                return DockerEngineResponse(status: .ok, contentType: "application/vnd.docker.raw-stream")
            } catch {
                await runtimeState.completeExec(id: id, exitCode: 127)
                throw error
            }
        }

        do {
            let output = DockerEngineExecStream(
                tty: exec.tty,
                attachStdin: exec.attachStdin,
                attachStdout: exec.attachStdout,
                attachStderr: exec.attachStderr
            )
            let process = try await ContainerClient().createProcess(
                containerId: exec.containerID,
                processId: UUID().uuidString.lowercased(),
                configuration: exec.process,
                stdio: output.stdio
            )
            try await process.start()
            output.closeOutputWriters()
            Task.detached { [runtimeState, output] in
                let exitCode = (try? await process.wait()) ?? 127
                output.finish()
                await runtimeState.completeExec(id: id, exitCode: exitCode)
            }
            return DockerEngineResponse(
                status: .switchingProtocols,
                contentType: exec.tty ? "application/vnd.docker.raw-stream" : "application/vnd.docker.multiplexed-stream",
                execStream: output
            )
        } catch {
            await runtimeState.completeExec(id: id, exitCode: 127)
            throw error
        }
    }

    private func inspectExec(id: String) async throws -> DockerEngineResponse {
        guard let exec = await runtimeState.exec(id: id) else {
            throw DockerAPIError.notFound("exec インスタンスが見つかりません")
        }
        return try Self.json(.ok, [
            "ID": exec.id,
            "ContainerID": exec.containerID,
            "Running": exec.running,
            "ExitCode": exec.exitCode as Any? ?? NSNull(),
            "OpenStdin": exec.running && exec.attachStdin,
            "OpenStdout": exec.running && exec.attachStdout,
            "OpenStderr": exec.running && exec.attachStderr,
            "ProcessConfig": [
                "privileged": false,
                "user": exec.process.user.description,
                "tty": exec.tty,
                "entrypoint": exec.process.executable,
                "arguments": exec.process.arguments,
            ],
        ] as [String: Any])
    }

    private func pullImage(query: [String: String], headers: HTTPHeaders) async throws -> DockerEngineResponse {
        guard let image = query["fromImage"], !image.isEmpty else {
            throw DockerAPIError.badRequest("fromImage query parameter が必要です")
        }
        let reference = query["tag"].flatMap { $0.isEmpty ? nil : "\(image):\($0)" } ?? image
        let registryCredentials = try Self.registryCredentials(
            header: headers.first(name: "X-Registry-Auth"),
            image: reference,
            containerSystemConfig: containerSystemConfig
        )
        let pulledImage = try await ClientImage.pull(
            reference: reference,
            containerSystemConfig: containerSystemConfig,
            registryCredentials: registryCredentials
        )
        _ = try await pulledImage.getCreateSnapshot(platform: .current)
        let message = try JSONSerialization.data(withJSONObject: ["status": "Pull complete", "id": pulledImage.digest])
        return DockerEngineResponse(status: .ok, body: message + Data([0x0A]))
    }

    private static func registryCredentials(
        header: String?,
        image: String,
        containerSystemConfig: ContainerSystemConfig
    ) throws -> RegistryCredentials? {
        guard let header, !header.isEmpty else { return nil }
        var encoded = header.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        while !encoded.count.isMultiple(of: 4) {
            encoded.append("=")
        }
        guard let data = Data(base64Encoded: encoded, options: .ignoreUnknownCharacters) else {
            throw DockerAPIError.badRequest("X-Registry-Auth が base64 ではありません")
        }
        let auth = try JSONDecoder().decode(DockerRegistryAuth.self, from: data)
        if !(auth.identityToken ?? "").isEmpty || !(auth.registryToken ?? "").isEmpty {
            throw DockerAPIError.unsupported("identity token と registry token による pull はサポートされていません")
        }

        var username = auth.username ?? ""
        var password = auth.password ?? ""
        if username.isEmpty, let encodedAuth = auth.auth, !encodedAuth.isEmpty {
            guard let credentialData = Data(base64Encoded: encodedAuth, options: .ignoreUnknownCharacters),
                let credential = String(data: credentialData, encoding: .utf8),
                let separator = credential.firstIndex(of: ":")
            else {
                throw DockerAPIError.badRequest("X-Registry-Auth の auth 値が正しくありません")
            }
            username = String(credential[..<separator])
            password = String(credential[credential.index(after: separator)...])
        }
        guard !username.isEmpty || !password.isEmpty else { return nil }

        if let serverAddress = auth.serverAddress, !serverAddress.isEmpty {
            let normalized = try ClientImage.normalizeReference(image, containerSystemConfig: containerSystemConfig)
            guard let imageDomain = try Reference.parse(normalized).resolvedDomain else {
                throw DockerAPIError.badRequest("pull 対象の registry を判定できません")
            }
            let authDomain = Self.registryDomain(serverAddress)
            let imageRegistry = Self.registryDomain(imageDomain)
            let dockerHubDomains: Set<String> = ["docker.io", "index.docker.io", "registry-1.docker.io"]
            guard authDomain == imageRegistry || (dockerHubDomains.contains(authDomain) && dockerHubDomains.contains(imageRegistry)) else {
                throw DockerAPIError.badRequest("X-Registry-Auth の registry が pull 対象と一致しません")
            }
        }
        return RegistryCredentials(username: username, password: password)
    }

    private static func registryDomain(_ value: String) -> String {
        let url = URLComponents(string: value)
        let host: String
        if let urlHost = url?.host {
            host = url?.port.map { "\(urlHost):\($0)" } ?? urlHost
        } else {
            host = value.split(separator: "/", maxSplits: 1).first.map(String.init) ?? value
        }
        return Reference.resolveDomain(domain: host).lowercased()
    }

    private func listImages() async throws -> DockerEngineResponse {
        let images = try await ClientImage.list()
        let results = await images.asyncMap { image in
            let manifest = try? await image.manifest(for: .current)
            let imageConfig = try? await image.config(for: .current)
            let imageID = Self.dockerImageID(manifest?.config.digest ?? image.digest)
            let size = manifest.map(Self.dockerImageSize) ?? image.descriptor.size
            return [
                "Id": imageID,
                "ParentId": "",
                "RepoTags": [image.reference],
                "RepoDigests": ["\(image.reference)@\(image.digest)"],
                "Created": Self.dockerImageCreated(imageConfig?.created),
                "Size": size,
                "SharedSize": -1,
                "VirtualSize": size,
                "Containers": -1,
                "Labels": imageConfig?.config?.labels ?? [:],
            ] as [String: Any]
        }
        return try Self.json(.ok, results)
    }

    private func inspectImage(name: String) async throws -> DockerEngineResponse {
        let image = try await ClientImage.get(reference: name, containerSystemConfig: containerSystemConfig)
        let manifest = try await image.manifest(for: .current)
        let imageConfig = try await image.config(for: .current)
        let imageID = Self.dockerImageID(manifest.config.digest)
        return try Self.json(.ok, [
            "Id": imageID,
            "RepoTags": [image.reference],
            "RepoDigests": ["\(image.reference)@\(image.digest)"],
            "Created": Self.dockerImageCreatedString(imageConfig.created),
            "Size": Self.dockerImageSize(manifest),
            "Architecture": imageConfig.architecture,
            "Os": imageConfig.os,
            "Config": [
                "Cmd": imageConfig.config?.cmd ?? [],
                "Entrypoint": imageConfig.config?.entrypoint ?? [],
                "Env": imageConfig.config?.env ?? [],
                "WorkingDir": imageConfig.config?.workingDir ?? "",
                "User": imageConfig.config?.user ?? "",
                "Labels": imageConfig.config?.labels ?? [:],
            ],
            "RootFS": ["Type": imageConfig.rootfs.type, "Layers": imageConfig.rootfs.diffIDs],
        ] as [String: Any])
    }

    private static func dockerImageID(_ digest: String) -> String {
        digest.hasPrefix("sha256:") ? digest : "sha256:\(digest)"
    }

    private static func dockerImageSize(_ manifest: Manifest) -> Int64 {
        ([manifest.config.size] + manifest.layers.map(\.size)).reduce(Int64(0)) { total, size in
            let (result, overflow) = total.addingReportingOverflow(size)
            return overflow ? Int64.max : result
        }
    }

    private static func dockerImageCreated(_ value: String?) -> Int64 {
        guard let date = value.flatMap(ISO8601DateFormatter().date(from:)) else { return 0 }
        return Int64(date.timeIntervalSince1970)
    }

    private static func dockerImageCreatedString(_ value: String?) -> String {
        guard let value, ISO8601DateFormatter().date(from: value) != nil else { return "1970-01-01T00:00:00Z" }
        return value
    }

    private func createNetwork(body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerNetworkCreateRequest.self, from: body)
        guard NetworkResource.nameValid(request.name) else {
            throw DockerAPIError.badRequest("ネットワーク名が正しくありません")
        }
        let driver = request.driver.flatMap { $0.isEmpty ? nil : $0.lowercased() }
        guard driver == nil || driver == "bridge" || driver == "container-network-vmnet" else {
            throw DockerAPIError.unsupported("Docker network driver \(driver ?? "") はサポートされていません")
        }
        let labels = try ResourceLabels(request.labels ?? [:])
        let ipv4 = try request.ipam?.config?.first?.subnet.map { try CIDRv4($0) }
        let configuration = try NetworkConfiguration(
            name: request.name,
            mode: request.isInternal == true ? .hostOnly : .nat,
            ipv4Subnet: ipv4,
            labels: labels,
            plugin: "container-network-vmnet"
        )
        let network = try await NetworkClient().create(configuration: configuration)
        return try Self.json(.created, ["Id": network.id, "Warning": ""])
    }

    private func listNetworks() async throws -> DockerEngineResponse {
        let networks = try await NetworkClient().list()
        return try Self.json(.ok, networks.map(Self.networkInspection))
    }

    private func inspectNetwork(id: String) async throws -> DockerEngineResponse {
        let network = try await NetworkClient().get(id: id)
        return try Self.json(.ok, Self.networkInspection(network))
    }

    private func createVolume(body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerVolumeCreateRequest.self, from: body)
        let volume = try await ClientVolume.create(
            name: request.name.flatMap { $0.isEmpty ? nil : $0 } ?? UUID().uuidString.lowercased(),
            driver: request.driver ?? "local",
            driverOpts: request.driverOpts ?? [:],
            labels: request.labels ?? [:]
        )
        return try Self.json(.created, Self.volumeInspection(volume))
    }

    private func listVolumes() async throws -> DockerEngineResponse {
        let volumes = try await ClientVolume.list()
        return try Self.json(.ok, [
            "Volumes": volumes.map(Self.volumeInspection),
            "Warnings": [],
        ] as [String: Any])
    }

    private func inspectVolume(name: String) async throws -> DockerEngineResponse {
        let volume = try await ClientVolume.inspect(name)
        return try Self.json(.ok, Self.volumeInspection(volume))
    }

    private static func containerSummary(
        _ snapshot: ContainerSnapshot,
        health: DockerEngineHealthSnapshot?,
        restartPolicy: DockerRestartPolicy,
        restartCount: Int,
        privileged: Bool,
        imageID: String
    ) -> [String: Any] {
        let config = snapshot.configuration
        let process = config.initProcess
        let state = snapshot.status == .running ? "running" : (snapshot.startedDate == nil ? "created" : "exited")
        let status = health.map { state == "running" ? "Up (\($0.status.rawValue))" : state } ?? state
        return [
            "Id": snapshot.id,
            "Names": ["/\(snapshot.id)"],
            "Image": config.image.reference,
            "ImageID": imageID,
            "Command": ([process.executable] + process.arguments).joined(separator: " "),
            "Created": Int64(config.creationDate.timeIntervalSince1970),
            "Ports": publishedPortSummary(config.publishedPorts),
            "Labels": config.labels,
            "State": state,
            "Status": status,
            "HostConfig": [
                "NetworkMode": config.networks.first?.network ?? "default",
                "Init": config.useInit,
                "RestartPolicy": restartPolicy.dockerJSON,
                "Privileged": privileged,
            ],
            "RestartCount": restartCount,
            "NetworkSettings": ["Networks": networkSettings(snapshot.networks, configured: config.networks, baseDomain: config.dns?.domain ?? fallbackDNSDomain)],
        ]
    }

    private static func containerInspection(
        _ snapshot: ContainerSnapshot,
        runtime: DockerEngineExitState?,
        healthcheck: DockerHealthcheck?,
        health: DockerEngineHealthSnapshot?,
        restartPolicy: DockerRestartPolicy,
        restartCount: Int,
        privileged: Bool,
        imageID: String
    ) -> [String: Any] {
        let config = snapshot.configuration
        let process = config.initProcess
        let startedAt = snapshot.startedDate.map { ISO8601DateFormatter().string(from: $0) } ?? "0001-01-01T00:00:00Z"
        let running = snapshot.status == .running
        let stateStatus = snapshot.status == .running ? "running" : (snapshot.startedDate == nil ? "created" : "exited")
        let exitCode = runtime?.exitCode ?? (running || snapshot.startedDate == nil ? 0 : -1)
        let finishedAt = runtime.map { ISO8601DateFormatter().string(from: $0.finishedAt) } ?? "0001-01-01T00:00:00Z"
        let env = process.environment
        let mounts = config.mounts.map { mount -> [String: Any] in
            ["Type": mount.isVolume ? "volume" : (mount.isTmpfs ? "tmpfs" : "bind"), "Source": mount.source, "Destination": mount.destination, "Mode": mount.options.joined(separator: ","), "RW": !mount.options.readonly, "Propagation": "rprivate"]
        }
        var state: [String: Any] = ["Status": stateStatus, "Running": running, "Paused": false, "Restarting": false, "OOMKilled": false, "Dead": false, "Pid": 0, "ExitCode": exitCode, "Error": "", "StartedAt": startedAt, "FinishedAt": finishedAt]
        if let health {
            state["Health"] = health.dockerJSON
        }
        var containerConfig: [String: Any] = [
            "Hostname": config.networks.first?.options.hostname.split(separator: ".", maxSplits: 1).first.map(String.init) ?? snapshot.id,
            "User": process.user.description,
            "Env": env,
            "Cmd": [process.executable] + process.arguments,
            "Image": config.image.reference,
            "WorkingDir": process.workingDirectory,
            "Entrypoint": [process.executable],
            "ExposedPorts": exposedPorts(config.publishedPorts),
            "Labels": config.labels,
            "Tty": process.terminal,
            "OpenStdin": false,
            "StopSignal": config.stopSignal ?? "SIGTERM",
        ]
        if let healthcheck {
            containerConfig["Healthcheck"] = healthcheck.dockerJSON
        }
        return [
            "Id": snapshot.id,
            "Created": ISO8601DateFormatter().string(from: config.creationDate),
            "Path": process.executable,
            "Args": process.arguments,
            "State": state,
            "Image": imageID,
            "Name": "/\(snapshot.id)",
            "RestartCount": restartCount,
            "Driver": "containerization",
            "Platform": "linux",
            "MountLabel": "",
            "ProcessLabel": "",
            "AppArmorProfile": "",
            "ExecIDs": NSNull(),
            "HostConfig": [
                "NetworkMode": config.networks.first?.network ?? "default",
                "Init": config.useInit,
                "ReadonlyRootfs": config.readOnly,
                "PortBindings": publishedPortBindings(config.publishedPorts),
                "Binds": [],
                "RestartPolicy": restartPolicy.dockerJSON,
                "Privileged": privileged,
            ],
            "GraphDriver": ["Name": "containerization", "Data": [:]],
            "Config": containerConfig,
            "NetworkSettings": ["Networks": networkSettings(snapshot.networks, configured: config.networks, baseDomain: config.dns?.domain ?? fallbackDNSDomain), "Ports": publishedPortBindings(config.publishedPorts)],
            "Mounts": mounts,
        ]
    }

    private static func publishedPortSummary(_ ports: [PublishPort]) -> [[String: Any]] {
        ports.flatMap { port in
            (0..<port.count).map { offset in
                [
                    "IP": port.hostAddress.description,
                    "PrivatePort": Int(port.containerPort + offset),
                    "PublicPort": Int(port.hostPort + offset),
                    "Type": port.proto.rawValue,
                ] as [String: Any]
            }
        }
    }

    private static func publishedPortBindings(_ ports: [PublishPort]) -> [String: Any] {
        var result = [String: [[String: String]]]()
        for port in ports {
            for offset in 0..<port.count {
                let key = "\(port.containerPort + offset)/\(port.proto.rawValue)"
                result[key, default: []].append([
                    "HostIp": port.hostAddress.description,
                    "HostPort": "\(port.hostPort + offset)",
                ])
            }
        }
        return result.mapValues { $0 as Any }
    }

    private static func exposedPorts(_ ports: [PublishPort]) -> [String: Any] {
        var result = [String: [String: String]]()
        for port in ports {
            for offset in 0..<port.count {
                result["\(port.containerPort + offset)/\(port.proto.rawValue)"] = [:]
            }
        }
        return result.mapValues { $0 as Any }
    }

    private static func networkSettings(
        _ attachments: [Attachment],
        configured: [AttachmentConfiguration],
        baseDomain: String
    ) -> [String: Any] {
        guard !attachments.isEmpty else {
            return Dictionary(uniqueKeysWithValues: configured.map { attachment in
                (attachment.network, [
                    "NetworkID": attachment.network,
                    "EndpointID": "",
                    "Gateway": "",
                    "IPAddress": "",
                    "IPPrefixLen": 0,
                    "MacAddress": "",
                    "Aliases": [attachment.options.hostname] + attachment.options.aliases.map {
                        Self.unqualifyNetworkAlias($0, network: attachment.network, baseDomain: baseDomain)
                    },
                ] as [String: Any])
            })
        }

        let configuredByNetwork = Dictionary(uniqueKeysWithValues: configured.map { ($0.network, $0) })
        return Dictionary(uniqueKeysWithValues: attachments.map { attachment in
            let aliases = [attachment.hostname] + (configuredByNetwork[attachment.network]?.options.aliases ?? []).map {
                Self.unqualifyNetworkAlias($0, network: attachment.network, baseDomain: baseDomain)
            }
            let uniqueAliases = aliases.reduce(into: [String]()) { result, alias in
                if !result.contains(where: { AttachmentOptions.canonicalHostname($0) == AttachmentOptions.canonicalHostname(alias) }) {
                    result.append(alias)
                }
            }
            return (attachment.network, [
                "NetworkID": attachment.network,
                "EndpointID": "",
                "Gateway": attachment.ipv4Gateway.description,
                "IPAddress": String(attachment.ipv4Address.address.description),
                "IPPrefixLen": attachment.ipv4Address.prefix.length,
                "MacAddress": attachment.macAddress?.description ?? "",
                "Aliases": uniqueAliases,
            ] as [String: Any])
        })
    }

    private static func networkInspection(_ network: NetworkResource) -> [String: Any] {
        [
            "Name": network.id,
            "Id": network.id,
            "Created": ISO8601DateFormatter().string(from: network.creationDate),
            "Scope": "local",
            "Driver": "bridge",
            "EnableIPv6": network.configuration.ipv6Subnet != nil,
            "Internal": network.configuration.mode == .hostOnly,
            "Attachable": true,
            "Ingress": false,
            "ConfigOnly": false,
            "ConfigFrom": ["Network": ""],
            "IPAM": ["Driver": "default", "Options": [:], "Config": network.configuration.ipv4Subnet.map { [["Subnet": $0.description, "Gateway": ""]] } ?? []],
            "Containers": [:],
            "Options": network.configuration.options,
            "Labels": network.configuration.labels.dictionary,
        ]
    }

    private static func volumeInspection(_ volume: VolumeConfiguration) -> [String: Any] {
        ["Name": volume.name, "Driver": volume.driver, "Mountpoint": volume.source, "CreatedAt": ISO8601DateFormatter().string(from: volume.creationDate), "Labels": volume.labels, "Scope": "local", "Options": volume.options, "Status": NSNull()]
    }

    private static func containerFilters(_ value: String?) throws -> ContainerListFilters {
        guard let value, !value.isEmpty else { return .all }
        let filterObject: Any
        do {
            filterObject = try JSONSerialization.jsonObject(with: Data(value.utf8))
        } catch {
            throw DockerAPIError.badRequest("filters は有効な JSON である必要があります")
        }
        guard let decoded = filterObject as? [String: Any] else {
            throw DockerAPIError.badRequest("filters は JSON object である必要があります")
        }
        var labels = [String: String]()
        if let rawLabels = decoded["label"] {
            if let labelFilters = rawLabels as? [String] {
                for label in labelFilters {
                    try Self.addLabelFilter(label, to: &labels)
                }
            } else if let labelFilter = rawLabels as? String {
                try Self.addLabelFilter(labelFilter, to: &labels)
            } else if let labelFilters = rawLabels as? [String: Any] {
                for (key, rawValues) in labelFilters {
                    if let values = rawValues as? [String] {
                        if values.isEmpty {
                            labels[key] = ".+"
                        } else {
                            for value in values {
                                try Self.addLabelFilter("\(key)=\(value)", to: &labels)
                            }
                        }
                    } else if let value = rawValues as? String {
                        try Self.addLabelFilter("\(key)=\(value)", to: &labels)
                    } else if let values = rawValues as? [String: Any] {
                        for (value, includeValue) in values {
                            guard let includeValue = includeValue as? Bool else {
                                throw DockerAPIError.badRequest("filters.label object の各値は boolean である必要があります")
                            }
                            try Self.addLabelFilter("\(key)=\(value)", to: &labels, include: includeValue)
                        }
                    } else if let include = rawValues as? Bool {
                        try Self.addLabelFilter(key, to: &labels, include: include)
                    } else {
                        throw DockerAPIError.badRequest("filters.label の値は文字列または文字列配列である必要があります")
                    }
                }
            } else {
                throw DockerAPIError.badRequest("filters.label は文字列配列または object である必要があります")
            }
        }
        let statuses = try Self.stringFilterValues(decoded["status"], field: "status")
        let status: RuntimeStatus? = statuses.contains("running") ? .running : (statuses.contains("exited") || statuses.contains("stopped") ? .stopped : nil)
        return ContainerListFilters(status: status, labels: labels)
    }

    private static func stringFilterValues(_ value: Any?, field: String) throws -> [String] {
        guard let value else { return [] }
        if let values = value as? [String] { return values }
        if let singleValue = value as? String { return [singleValue] }
        throw DockerAPIError.badRequest("filters.\(field) は文字列配列である必要があります")
    }

    private static func addLabelFilter(_ value: String, to labels: inout [String: String], include: Bool = true) throws {
        let parts = value.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false).map(String.init)
        guard let key = parts.first, !key.isEmpty else {
            throw DockerAPIError.badRequest("filters.label に空のキーは指定できません")
        }
        if parts.count == 2 {
            let expectedValue = NSRegularExpression.escapedPattern(for: parts[1])
            let pattern = include ? "^\(expectedValue)$" : ContainerListFilters.exclude(expectedValue)
            labels[key] = labels[key].map { "(?:\($0))|(?:\(pattern))" } ?? pattern
        } else {
            let pattern = include ? ".+" : "^$"
            labels[key] = labels[key].map { "(?:\($0))|(?:\(pattern))" } ?? pattern
        }
    }

    private static func qualify(_ name: String, domain: String?) -> String {
        guard let domain, !domain.isEmpty, !name.contains(".") else { return name }
        return "\(name).\(domain)."
    }

    private static func dockerDNSDomain(_ config: ContainerSystemConfig) -> String {
        for configured in [config.container.dns?.domain, config.dns.domain].compactMap({ $0 }) {
            let domain = configured.trimmingCharacters(in: CharacterSet(charactersIn: "."))
            if !domain.isEmpty {
                return domain.lowercased()
            }
        }
        return Self.fallbackDNSDomain
    }

    private static func networkDNSDomain(network: String, baseDomain: String) -> String {
        let prefix = String(
            network.lowercased().utf8.prefix(24).map { byte -> Character in
                if (48...57).contains(byte) || (97...122).contains(byte) {
                    return Character(UnicodeScalar(byte))
                }
                return "-"
            }
        ).trimmingCharacters(in: CharacterSet(charactersIn: "-"))
        let hash = network.utf8.reduce(UInt64(14_695_981_039_346_656_037)) { ($0 ^ UInt64($1)) &* 1_099_511_628_211 }
        let networkLabel = "net-\(prefix.isEmpty ? "x" : prefix)-\(String(hash, radix: 16))"
        return "\(networkLabel).\(baseDomain.trimmingCharacters(in: CharacterSet(charactersIn: ".")))"
    }

    private static func qualifyNetworkAlias(_ alias: String, network: String, baseDomain: String) -> String {
        let name = alias.trimmingCharacters(in: CharacterSet(charactersIn: "."))
        return "\(name).\(networkDNSDomain(network: network, baseDomain: baseDomain))."
    }

    private static func unqualifyNetworkAlias(_ alias: String, network: String, baseDomain: String) -> String {
        let name = alias.hasSuffix(".") ? String(alias.dropLast()) : alias
        let suffix = ".\(networkDNSDomain(network: network, baseDomain: baseDomain))"
        guard name.hasSuffix(suffix) else { return alias }
        return String(name.dropLast(suffix.count))
    }

    private static func validateUnsupportedCreateConfig(body: Data) throws {
        guard let request = try JSONSerialization.jsonObject(with: body) as? [String: Any] else { return }

        if let healthcheck = request["Healthcheck"] as? [String: Any] {
            let supportedHealthcheckFields: Set<String> = ["Test", "Interval", "Timeout", "StartPeriod", "StartInterval", "Retries"]
            for (key, value) in healthcheck where !supportedHealthcheckFields.contains(key) && hasNonDefaultValue(value, field: key) {
                throw DockerAPIError.unsupported("Healthcheck.\(key) はサポートされていません")
            }
        }

        guard let hostConfig = request["HostConfig"] as? [String: Any] else { return }

        let supported: Set<String> = [
            "Binds", "Mounts", "PortBindings", "NetworkMode", "Memory", "NanoCpus",
            "ReadonlyRootfs", "ShmSize", "Dns", "DnsSearch", "DnsOptions", "Sysctls", "Tmpfs", "Init", "RestartPolicy", "Privileged", "CapAdd", "CapDrop",
        ]
        for (key, value) in hostConfig where !supported.contains(key) && hasNonDefaultValue(value, field: key) {
            throw DockerAPIError.unsupported("HostConfig.\(key) が指定されていますが、この API ではサポートされていません")
        }

        if let restartPolicy = hostConfig["RestartPolicy"] as? [String: Any] {
            let supportedRestartPolicyFields: Set<String> = ["Name", "MaximumRetryCount"]
            for (key, value) in restartPolicy where !supportedRestartPolicyFields.contains(key) && hasNonDefaultValue(value, field: key) {
                throw DockerAPIError.unsupported("HostConfig.RestartPolicy.\(key) はサポートされていません")
            }
        }

        if let mounts = hostConfig["Mounts"] as? [[String: Any]] {
            let supportedMountFields: Set<String> = ["Type", "Source", "Target", "ReadOnly", "Consistency", "BindOptions", "VolumeOptions", "TmpfsOptions"]
            for (index, mount) in mounts.enumerated() {
                for (key, value) in mount where !supportedMountFields.contains(key) && hasNonDefaultValue(value, field: key) {
                    throw DockerAPIError.unsupported("HostConfig.Mounts[\(index)].\(key) はサポートされていません")
                }
                if let consistency = mount["Consistency"] as? String, !consistency.isEmpty, consistency != "consistent" {
                    throw DockerAPIError.unsupported("HostConfig.Mounts[\(index)].Consistency はサポートされていません")
                }
                for (key, allowed) in [
                    ("BindOptions", Set(["Propagation", "NonRecursive", "CreateMountpoint", "ReadOnlyNonRecursive", "ReadOnlyForceRecursive"])),
                    ("VolumeOptions", Set(["NoCopy", "Labels", "DriverConfig", "Subpath"])),
                    ("TmpfsOptions", Set(["SizeBytes", "Mode", "Options"])),
                ] {
                    guard let options = mount[key] as? [String: Any] else { continue }
                    for (option, value) in options {
                        guard allowed.contains(option) else {
                            if hasNonDefaultValue(value, field: option) {
                                throw DockerAPIError.unsupported("HostConfig.Mounts[\(index)].\(key).\(option) はサポートされていません")
                            }
                            continue
                        }
                        if key == "BindOptions", option == "Propagation", ["", "rprivate"].contains(value as? String ?? "") {
                            continue
                        }
                        if hasNonDefaultValue(value, field: option) {
                            throw DockerAPIError.unsupported("HostConfig.Mounts[\(index)].\(key).\(option) はサポートされていません")
                        }
                    }
                }
            }
        }
    }

    private static func hasNonDefaultValue(_ value: Any, field: String? = nil) -> Bool {
        if let value = value as? [String: Any] {
            if field == "RestartPolicy" {
                let name = value["Name"] as? String ?? ""
                let retryCount = (value["MaximumRetryCount"] as? NSNumber)?.intValue ?? 0
                return !["", "no"].contains(name) || retryCount != 0
            }
            if field == "LogConfig" {
                let type = value["Type"] as? String ?? ""
                let config = value["Config"] as? [String: Any] ?? [:]
                return !["", "json-file", "local"].contains(type) || !config.isEmpty
            }
            if field == "CgroupnsMode" {
                let mode = value["Mode"] as? String ?? ""
                return !["", "private"].contains(mode)
            }
            return value.contains { hasNonDefaultValue($0.value, field: $0.key) }
        }
        if let values = value as? [Any] {
            return values.contains { hasNonDefaultValue($0) }
        }
        if value is NSNull {
            return false
        }
        if let number = value as? NSNumber {
            return number.doubleValue != 0
        }
        if let string = value as? String {
            if field == "CgroupnsMode" {
                return !["", "private"].contains(string)
            }
            return !string.isEmpty
        }
        return true
    }

    private static func status(for error: Error) -> HTTPResponseStatus {
        if error is DecodingError {
            return .badRequest
        }
        if let error = error as? DockerAPIError {
            switch error {
            case .badRequest: return .badRequest
            case .notFound: return .notFound
            case .conflict: return .conflict
            case .unsupported: return .notImplemented
            case .internalError: return .internalServerError
            }
        }
        if let error = error as? VolumeError {
            switch error {
            case .volumeNotFound: return .notFound
            case .volumeAlreadyExists, .volumeInUse: return .conflict
            case .invalidVolumeName: return .badRequest
            case .driverNotSupported: return .notImplemented
            case .storageError: return .internalServerError
            }
        }
        if let error = error as? ContainerizationError {
            if error.isCode(.notFound) { return .notFound }
            if error.isCode(.exists) { return .conflict }
            if error.isCode(.invalidArgument) { return .badRequest }
        }
        return .internalServerError
    }

    private static func error(_ status: HTTPResponseStatus, _ message: String) -> DockerEngineResponse {
        (try? json(status, ["message": message])) ?? DockerEngineResponse(status: status)
    }

    private static func json(_ status: HTTPResponseStatus, _ value: Any) throws -> DockerEngineResponse {
        DockerEngineResponse(status: status, body: try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed]))
    }
}

private enum DockerAPIError: LocalizedError {
    case badRequest(String)
    case notFound(String)
    case conflict(String)
    case unsupported(String)
    case internalError(String)

    var errorDescription: String? {
        switch self {
        case .badRequest(let message), .notFound(let message), .conflict(let message), .unsupported(let message), .internalError(let message):
            message
        }
    }
}

private struct DockerCreateRequest: Decodable {
    let hostname: String?
    let image: String
    let cmd: [String]?
    let entrypoint: [String]?
    let env: [String]?
    let workingDir: String?
    let user: String?
    let labels: [String: String]?
    let tty: Bool?
    let stopSignal: String?
    let healthcheck: DockerHealthcheck?
    let hostConfig: DockerHostConfig?
    let networkingConfig: DockerNetworkingConfig?

    enum CodingKeys: String, CodingKey {
        case hostname = "Hostname"
        case image = "Image"
        case cmd = "Cmd"
        case entrypoint = "Entrypoint"
        case env = "Env"
        case workingDir = "WorkingDir"
        case user = "User"
        case labels = "Labels"
        case tty = "Tty"
        case stopSignal = "StopSignal"
        case healthcheck = "Healthcheck"
        case hostConfig = "HostConfig"
        case networkingConfig = "NetworkingConfig"
    }
}

private struct DockerExecCreateRequest: Decodable {
    let command: [String]?
    let environment: [String]?
    let workingDirectory: String?
    let user: String?
    let tty: Bool?
    let attachStdin: Bool?
    let attachStdout: Bool?
    let attachStderr: Bool?

    enum CodingKeys: String, CodingKey {
        case command = "Cmd"
        case environment = "Env"
        case workingDirectory = "WorkingDir"
        case user = "User"
        case tty = "Tty"
        case attachStdin = "AttachStdin"
        case attachStdout = "AttachStdout"
        case attachStderr = "AttachStderr"
    }
}

private struct DockerExecStartRequest: Decodable {
    let detach: Bool?
    init(detach: Bool? = nil) { self.detach = detach }
    enum CodingKeys: String, CodingKey { case detach = "Detach" }
}

private struct DockerHealthcheck: Decodable, Sendable {
    let test: [String]?
    let interval: Int64?
    let timeout: Int64?
    let startPeriod: Int64?
    let startInterval: Int64?
    let retries: Int?

    enum CodingKeys: String, CodingKey {
        case test = "Test"
        case interval = "Interval"
        case timeout = "Timeout"
        case startPeriod = "StartPeriod"
        case startInterval = "StartInterval"
        case retries = "Retries"
    }

    var isDisabled: Bool { test?.first == "NONE" }

    var command: (executable: String, arguments: [String])? {
        guard let test, let kind = test.first else { return nil }
        switch kind {
        case "CMD":
            guard test.count > 1, !test[1].isEmpty else { return nil }
            return (test[1], Array(test.dropFirst(2)))
        case "CMD-SHELL":
            let shellCommand = test.dropFirst().joined(separator: " ")
            guard !shellCommand.isEmpty else { return nil }
            return ("/bin/sh", ["-c", shellCommand])
        default:
            return nil
        }
    }

    var dockerJSON: [String: Any] {
        var result: [String: Any] = ["Test": test ?? []]
        if let interval { result["Interval"] = interval }
        if let timeout { result["Timeout"] = timeout }
        if let startPeriod { result["StartPeriod"] = startPeriod }
        if let startInterval { result["StartInterval"] = startInterval }
        if let retries { result["Retries"] = retries }
        return result
    }

    var intervalDuration: Duration { Self.duration(interval, default: 30_000_000_000) }
    var timeoutDuration: Duration { Self.duration(timeout, default: 30_000_000_000) }
    var startPeriodDuration: Duration { Self.duration(startPeriod, default: 0) }
    var startIntervalDuration: Duration { Self.duration(startInterval, default: 5_000_000_000) }
    var retryCount: Int { max(1, retries ?? 3) }

    private static func duration(_ value: Int64?, default defaultValue: Int64) -> Duration {
        let nanoseconds = value.flatMap { $0 > 0 ? $0 : nil } ?? defaultValue
        return .nanoseconds(nanoseconds)
    }
}

private struct DockerHostConfig: Decodable {
    let binds: [String]?
    let mounts: [DockerMount]?
    let portBindings: [String: [DockerPortBinding]?]?
    let networkMode: String?
    let memory: Int64?
    let nanoCPUs: Int64?
    let readonlyRootfs: Bool?
    let privileged: Bool?
    let capAdd: [String]?
    let capDrop: [String]?
    let useInit: Bool?
    let shmSize: Int64?
    let dns: [String]?
    let dnsSearch: [String]?
    let dnsOptions: [String]?
    let sysctls: [String: String]?
    let tmpfs: [String: String]?
    let restartPolicy: DockerRestartPolicy?

    init() {
        binds = nil
        mounts = nil
        portBindings = nil
        networkMode = nil
        memory = nil
        nanoCPUs = nil
        readonlyRootfs = nil
        privileged = nil
        capAdd = nil
        capDrop = nil
        useInit = nil
        shmSize = nil
        dns = nil
        dnsSearch = nil
        dnsOptions = nil
        sysctls = nil
        tmpfs = nil
        restartPolicy = nil
    }

    enum CodingKeys: String, CodingKey {
        case binds = "Binds"
        case mounts = "Mounts"
        case portBindings = "PortBindings"
        case networkMode = "NetworkMode"
        case memory = "Memory"
        case nanoCPUs = "NanoCpus"
        case readonlyRootfs = "ReadonlyRootfs"
        case privileged = "Privileged"
        case capAdd = "CapAdd"
        case capDrop = "CapDrop"
        case useInit = "Init"
        case shmSize = "ShmSize"
        case dns = "Dns"
        case dnsSearch = "DnsSearch"
        case dnsOptions = "DnsOptions"
        case sysctls = "Sysctls"
        case tmpfs = "Tmpfs"
        case restartPolicy = "RestartPolicy"
    }
}

private struct DockerRestartPolicy: Decodable, Sendable {
    let name: String
    let maximumRetryCount: Int

    init(name: String = "no", maximumRetryCount: Int = 0) {
        self.name = name
        self.maximumRetryCount = maximumRetryCount
    }

    enum CodingKeys: String, CodingKey {
        case name = "Name"
        case maximumRetryCount = "MaximumRetryCount"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decodeIfPresent(String.self, forKey: .name) ?? "no"
        maximumRetryCount = try container.decodeIfPresent(Int.self, forKey: .maximumRetryCount) ?? 0
    }

    var normalizedName: String { name.isEmpty ? "no" : name }

    var dockerJSON: [String: Any] {
        ["Name": normalizedName, "MaximumRetryCount": maximumRetryCount]
    }
}

private struct DockerPortBinding: Decodable {
    let hostIP: String?
    let hostPort: String?
    enum CodingKeys: String, CodingKey { case hostIP = "HostIp"; case hostPort = "HostPort" }
}

private struct DockerMount: Decodable {
    let type: String?
    let source: String?
    let target: String?
    let readOnly: Bool?
    enum CodingKeys: String, CodingKey { case type = "Type"; case source = "Source"; case target = "Target"; case readOnly = "ReadOnly" }
}

private struct DockerNetworkingConfig: Decodable {
    let endpointsConfig: [String: DockerEndpointConfig]?
    enum CodingKeys: String, CodingKey { case endpointsConfig = "EndpointsConfig" }
}

private struct DockerEndpointConfig: Decodable {
    let aliases: [String]?
    enum CodingKeys: String, CodingKey { case aliases = "Aliases" }
    init(aliases: [String]? = nil) { self.aliases = aliases }
}

private struct DockerNetworkCreateRequest: Decodable {
    let name: String
    let driver: String?
    let isInternal: Bool?
    let labels: [String: String]?
    let ipam: DockerIPAM?
    enum CodingKeys: String, CodingKey { case name = "Name"; case driver = "Driver"; case isInternal = "Internal"; case labels = "Labels"; case ipam = "IPAM" }
}

private struct DockerIPAM: Decodable {
    let config: [DockerIPAMConfig]?
    enum CodingKeys: String, CodingKey { case config = "Config" }
}

private struct DockerIPAMConfig: Decodable {
    let subnet: String?
    enum CodingKeys: String, CodingKey { case subnet = "Subnet" }
}

private struct DockerVolumeCreateRequest: Decodable {
    let name: String?
    let driver: String?
    let driverOpts: [String: String]?
    let labels: [String: String]?
    enum CodingKeys: String, CodingKey { case name = "Name"; case driver = "Driver"; case driverOpts = "DriverOpts"; case labels = "Labels" }
}

private struct DockerRegistryAuth: Decodable {
    let username: String?
    let password: String?
    let auth: String?
    let serverAddress: String?
    let identityToken: String?
    let registryToken: String?

    enum CodingKeys: String, CodingKey {
        case username
        case password
        case auth
        case serverAddress = "serveraddress"
        case identityToken = "identitytoken"
        case registryToken = "registrytoken"
    }
}

struct DockerEngineExitState: Sendable {
    let exitCode: Int32
    let finishedAt: Date
}

struct DockerEngineHealthSnapshot: Sendable {
    enum Status: String, Sendable {
        case starting
        case healthy
        case unhealthy
    }

    struct Log: Sendable {
        let startedAt: Date
        let completedAt: Date
        let exitCode: Int32
        let output: String
    }

    let status: Status
    let failingStreak: Int
    let log: [Log]

    var dockerJSON: [String: Any] {
        [
            "Status": status.rawValue,
            "FailingStreak": failingStreak,
            "Log": log.map { entry in
                [
                    "Start": ISO8601DateFormatter().string(from: entry.startedAt),
                    "End": ISO8601DateFormatter().string(from: entry.completedAt),
                    "ExitCode": entry.exitCode,
                    "Output": entry.output,
                ] as [String: Any]
            },
        ]
    }
}

private struct DockerEngineRestartPlan: Sendable {
    let delay: Duration
    let generation: Int
}

private struct DockerEngineHealthcheckConfiguration: Sendable {
    let check: DockerHealthcheck
    let process: ProcessConfiguration
}

private struct DockerEngineExecState: Sendable {
    let id: String
    let containerID: String
    let process: ProcessConfiguration
    let tty: Bool
    let attachStdin: Bool
    let attachStdout: Bool
    let attachStderr: Bool
    var running: Bool
    var exitCode: Int32?
}

private enum DockerEngineHealthcheckResult: Sendable {
    case completed(Int32, String)
    case timedOut(String)
    case failed(String)
    case cancelled
}

private final class DockerEngineHealthcheckOutput: @unchecked Sendable {
    private static let maxBytes = 4 * 1024

    let pipe = Pipe()
    private let lock = NSLock()
    private var data = Data()

    init() {
        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let chunk = handle.availableData
            guard !chunk.isEmpty else {
                handle.readabilityHandler = nil
                return
            }
            self?.append(chunk)
        }
    }

    func finish() async -> String {
        try? pipe.fileHandleForWriting.close()
        try? await Task.sleep(for: .milliseconds(10))
        pipe.fileHandleForReading.readabilityHandler = nil
        return String(decoding: snapshot(), as: UTF8.self)
    }

    private func snapshot() -> Data {
        lock.lock()
        defer { lock.unlock() }
        return data
    }

    private func append(_ chunk: Data) {
        lock.lock()
        defer { lock.unlock() }
        let remaining = Self.maxBytes - data.count
        if remaining > 0 {
            data.append(contentsOf: chunk.prefix(remaining))
        }
    }
}

actor DockerEngineRuntimeState {
    private var exits = [String: DockerEngineExitState]()
    private var execs = [String: DockerEngineExecState]()
    private var restartPolicies = [String: DockerRestartPolicy]()
    private var privilegedContainers = Set<String>()
    private var restartAttempts = [String: Int]()
    private var restartCounts = [String: Int]()
    private var generations = [String: Int]()
    private var manuallyStopped = Set<String>()
    private var restarting = Set<String>()
    private var configuredHealthchecks = [String: DockerHealthcheck]()
    private var healthchecks = [String: DockerEngineHealthcheckConfiguration]()
    private var healthStates = [String: DockerEngineHealthSnapshot]()
    private var healthTasks = [String: Task<Void, Never>]()

    func started(id: String, automaticRestart: Bool = false) {
        exits.removeValue(forKey: id)
        generations[id, default: 0] += 1
        restarting.remove(id)
        if automaticRestart {
            restartCounts[id, default: 0] += 1
        } else {
            manuallyStopped.remove(id)
            restartAttempts[id] = 0
        }
        healthTasks[id]?.cancel()
        guard let healthcheck = healthchecks[id] else { return }
        healthStates[id] = DockerEngineHealthSnapshot(status: .starting, failingStreak: 0, log: [])
        healthTasks[id] = Task.detached { [weak self] in
            await self?.monitorHealthcheck(id: id, configuration: healthcheck)
        }
    }

    fileprivate func exited(id: String, exitCode: Int32) -> DockerEngineRestartPlan? {
        exits[id] = DockerEngineExitState(exitCode: exitCode, finishedAt: Date())
        healthTasks.removeValue(forKey: id)?.cancel()
        guard !manuallyStopped.contains(id), let policy = restartPolicies[id] else { return nil }
        let shouldRestart: Bool
        switch policy.normalizedName {
        case "always", "unless-stopped":
            shouldRestart = true
        case "on-failure":
            shouldRestart = exitCode != 0
        default:
            shouldRestart = false
        }
        guard shouldRestart else { return nil }
        let attempts = restartAttempts[id, default: 0]
        if policy.normalizedName == "on-failure", policy.maximumRetryCount > 0, attempts >= policy.maximumRetryCount {
            return nil
        }
        restartAttempts[id] = attempts + 1
        let backoff = min(100 * (1 << min(attempts, 9)), 60_000)
        return DockerEngineRestartPlan(delay: .milliseconds(backoff), generation: generations[id, default: 0])
    }

    func state(id: String) -> DockerEngineExitState? {
        exits[id]
    }

    fileprivate func configureRestartPolicy(id: String, policy: DockerRestartPolicy, privileged: Bool) {
        generations[id, default: 0] += 1
        manuallyStopped.remove(id)
        restartAttempts[id] = 0
        restartCounts[id] = 0
        restartPolicies[id] = policy
        if privileged {
            privilegedContainers.insert(id)
        } else {
            privilegedContainers.remove(id)
        }
    }

    fileprivate func restartPolicy(id: String) -> DockerRestartPolicy {
        restartPolicies[id] ?? DockerRestartPolicy()
    }

    func restartCount(id: String) -> Int {
        restartCounts[id, default: 0]
    }

    func isPrivileged(id: String) -> Bool {
        privilegedContainers.contains(id)
    }

    func createExec(
        containerID: String,
        process: ProcessConfiguration,
        tty: Bool,
        attachStdin: Bool,
        attachStdout: Bool,
        attachStderr: Bool
    ) -> String {
        let id = UUID().uuidString.lowercased()
        execs[id] = DockerEngineExecState(
            id: id,
            containerID: containerID,
            process: process,
            tty: tty,
            attachStdin: attachStdin,
            attachStdout: attachStdout,
            attachStderr: attachStderr,
            running: false,
            exitCode: nil
        )
        return id
    }

    fileprivate func beginExec(id: String) -> DockerEngineExecState? {
        guard var exec = execs[id], !exec.running, exec.exitCode == nil else { return nil }
        exec.running = true
        execs[id] = exec
        return exec
    }

    fileprivate func exec(id: String) -> DockerEngineExecState? {
        execs[id]
    }

    func completeExec(id: String, exitCode: Int32) {
        guard var exec = execs[id] else { return }
        exec.running = false
        exec.exitCode = exitCode
        execs[id] = exec
    }

    func preventRestart(id: String) {
        manuallyStopped.insert(id)
        restarting.remove(id)
    }

    func allowRestart(id: String) {
        manuallyStopped.remove(id)
    }

    func beginRestart(id: String, generation: Int) -> Bool {
        guard restartPolicies[id] != nil,
            generations[id, default: 0] == generation,
            !manuallyStopped.contains(id), !restarting.contains(id)
        else { return false }
        restarting.insert(id)
        return true
    }

    func restartIsCurrent(id: String, generation: Int) -> Bool {
        restartPolicies[id] != nil && generations[id, default: 0] == generation && !manuallyStopped.contains(id) && restarting.contains(id)
    }

    func restartFailed(id: String, generation: Int) {
        guard generations[id, default: 0] == generation else { return }
        restarting.remove(id)
    }

    fileprivate func configureHealthcheck(id: String, healthcheck: DockerHealthcheck?, process: ProcessConfiguration) {
        if let healthcheck {
            configuredHealthchecks[id] = healthcheck
        } else {
            configuredHealthchecks.removeValue(forKey: id)
        }
        guard let healthcheck, !healthcheck.isDisabled, let command = healthcheck.command else {
            healthchecks.removeValue(forKey: id)
            healthStates.removeValue(forKey: id)
            healthTasks.removeValue(forKey: id)?.cancel()
            return
        }
        healthchecks[id] = DockerEngineHealthcheckConfiguration(
            check: healthcheck,
            process: ProcessConfiguration(
                executable: command.executable,
                arguments: command.arguments,
                environment: process.environment,
                workingDirectory: process.workingDirectory,
                terminal: false,
                user: process.user,
                supplementalGroups: process.supplementalGroups,
                rlimits: process.rlimits
            )
        )
        healthStates[id] = DockerEngineHealthSnapshot(status: .starting, failingStreak: 0, log: [])
    }

    fileprivate func healthcheck(id: String) -> DockerHealthcheck? {
        configuredHealthchecks[id]
    }

    fileprivate func health(id: String) -> DockerEngineHealthSnapshot? {
        healthStates[id]
    }

    func removed(id: String) {
        exits.removeValue(forKey: id)
        generations[id, default: 0] += 1
        restartPolicies.removeValue(forKey: id)
        privilegedContainers.remove(id)
        restartAttempts.removeValue(forKey: id)
        restartCounts.removeValue(forKey: id)
        manuallyStopped.insert(id)
        restarting.remove(id)
        healthTasks.removeValue(forKey: id)?.cancel()
        configuredHealthchecks.removeValue(forKey: id)
        healthchecks.removeValue(forKey: id)
        healthStates.removeValue(forKey: id)
    }

    private func monitorHealthcheck(id: String, configuration: DockerEngineHealthcheckConfiguration) async {
        let startPeriodEnds = Date().addingTimeInterval(Self.seconds(configuration.check.startPeriodDuration))
        while !Task.isCancelled {
            let checkStartedAt = Date()
            let result = await Self.runHealthcheck(id: id, configuration: configuration)
            guard !Task.isCancelled else { return }

            let current = healthStates[id] ?? DockerEngineHealthSnapshot(status: .starting, failingStreak: 0, log: [])
            let passed: Bool
            let output: String
            let exitCode: Int32
            switch result {
            case .completed(let resultExitCode, let resultOutput):
                exitCode = resultExitCode
                passed = resultExitCode == 0
                output = resultOutput
            case .timedOut(let resultOutput):
                passed = false
                exitCode = -1
                output = resultOutput.isEmpty ? "healthcheck timed out" : resultOutput
            case .failed(let message):
                passed = false
                exitCode = -1
                output = message
            case .cancelled:
                return
            }

            let completedAt = Date()
            var failingStreak = current.failingStreak
            let status: DockerEngineHealthSnapshot.Status
            if passed {
                failingStreak = 0
                status = .healthy
            } else if completedAt < startPeriodEnds && current.status != .healthy {
                failingStreak = 0
                status = .starting
            } else {
                failingStreak += 1
                if failingStreak >= configuration.check.retryCount {
                    status = .unhealthy
                } else {
                    status = current.status == .healthy ? .healthy : .starting
                }
            }
            let log = (current.log + [DockerEngineHealthSnapshot.Log(
                startedAt: checkStartedAt,
                completedAt: completedAt,
                exitCode: exitCode,
                output: output
            )]).suffix(5)
            healthStates[id] = DockerEngineHealthSnapshot(status: status, failingStreak: failingStreak, log: Array(log))

            do {
                let interval = Date() < startPeriodEnds ? configuration.check.startIntervalDuration : configuration.check.intervalDuration
                try await Task.sleep(for: interval)
            } catch {
                return
            }
        }
    }

    private static func runHealthcheck(id: String, configuration: DockerEngineHealthcheckConfiguration) async -> DockerEngineHealthcheckResult {
        let client = ContainerClient()
        let output = DockerEngineHealthcheckOutput()
        let process: ClientProcess
        do {
            process = try await client.createProcess(
                containerId: id,
                processId: "healthcheck-\(UUID().uuidString.lowercased())",
                configuration: configuration.process,
                stdio: [nil, output.pipe.fileHandleForWriting, output.pipe.fileHandleForWriting]
            )
            try await process.start()
        } catch {
            let processOutput = await output.finish()
            return .failed([processOutput, String(describing: error)].filter { !$0.isEmpty }.joined(separator: "\n"))
        }

        let result = await withTaskGroup(of: DockerEngineHealthcheckResult.self) { group in
            group.addTask {
                do {
                    let exitCode = try await process.wait()
                    return .completed(exitCode, await output.finish())
                } catch {
                    return .failed([await output.finish(), String(describing: error)].filter { !$0.isEmpty }.joined(separator: "\n"))
                }
            }
            group.addTask {
                do {
                    try await Task.sleep(for: configuration.check.timeoutDuration)
                    try? await process.kill(9)
                    _ = try? await process.wait()
                    let processOutput = await output.finish()
                    return .timedOut(processOutput)
                } catch {
                    return .cancelled
                }
            }
            let first = await group.next() ?? .failed("healthcheck process did not complete")
            group.cancelAll()
            switch first {
            case .timedOut, .cancelled:
                try? await process.kill(9)
                _ = try? await process.wait()
                _ = await output.finish()
            default:
                break
            }
            return first
        }
        return result
    }

    private static func seconds(_ duration: Duration) -> TimeInterval {
        let components = duration.components
        return TimeInterval(components.seconds) + TimeInterval(components.attoseconds) / 1_000_000_000_000_000_000
    }
}

private extension Sequence {
    func asyncMap<T>(_ transform: (Element) async throws -> T) async rethrows -> [T] {
        var values = [T]()
        values.reserveCapacity(underestimatedCount)
        for element in self {
            values.append(try await transform(element))
        }
        return values
    }
}
