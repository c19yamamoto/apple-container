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
import ContainerPersistence
import ContainerResource
import Containerization
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

    let containerSystemConfig: ContainerSystemConfig
    let log: Logger
    let runtimeState: DockerEngineRuntimeState

    func handle(method: HTTPMethod, uri: String, headers: HTTPHeaders, body: Data) async -> DockerEngineResponse {
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
            if method == .POST && path == "/containers/create" {
                return try await createContainer(query: query, body: body)
            }
            if pathParts.count == 3 && pathParts[0] == "containers" {
                let id = pathParts[1]
                if method == .GET && pathParts[2] == "json" {
                    return try await inspectContainer(id: id)
                }
                if method == .POST && pathParts[2] == "start" {
                    try await startContainer(id: id)
                    return DockerEngineResponse(status: .noContent)
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
            }
            if pathParts.count == 2 && pathParts[0] == "containers" {
                let id = pathParts[1]
                if method == .DELETE {
                    try await ContainerClient().delete(id: id, force: query["force"] == "1" || query["force"] == "true")
                    await runtimeState.removed(id: id)
                    return DockerEngineResponse(status: .noContent)
                }
            }
            if method == .POST && path == "/images/create" {
                return try await pullImage(query: query, headers: headers)
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
        return try Self.json(.ok, snapshots.map(Self.containerSummary))
    }

    private func createContainer(query: [String: String], body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerCreateRequest.self, from: body)
        try Self.validateUnsupportedCreateConfig(body: body)
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
        config.resources = try Parser.resources(
            cpus: nil,
            memory: nil,
            defaultCPUs: containerSystemConfig.container.cpus,
            defaultMemory: containerSystemConfig.container.memory
        )
        config.readOnly = host.readonlyRootfs ?? false
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

    private func startContainer(id: String) async throws {
        let client = ContainerClient()
        let process = try await client.bootstrap(id: id, stdio: [nil, nil, nil])
        try await process.start()
        await runtimeState.started(id: id)
        let runtimeState = self.runtimeState
        Task {
            if let exitCode = try? await process.wait() {
                await runtimeState.exited(id: id, exitCode: exitCode)
            }
        }
    }

    private func stopContainer(id: String, timeout: String?) async throws {
        let seconds = timeout.flatMap(Int32.init) ?? ContainerStopOptions.default.timeoutInSeconds
        try await ContainerClient().stop(id: id, opts: ContainerStopOptions(timeoutInSeconds: seconds, signal: nil))
    }

    private func inspectContainer(id: String) async throws -> DockerEngineResponse {
        let snapshot = try await ContainerClient().get(id: id)
        let runtime = await runtimeState.state(id: id)
        return try Self.json(.ok, Self.containerInspection(snapshot, runtime: runtime))
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
        let image = try await ClientImage.pull(
            reference: reference,
            containerSystemConfig: containerSystemConfig,
            registryCredentials: registryCredentials
        )
        _ = try await image.getCreateSnapshot(platform: .current)
        let message = try JSONSerialization.data(withJSONObject: ["status": "Pull complete", "id": image.digest])
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
        let results = try await images.asyncMap { image in
            [
                "Id": "sha256:\(image.digest.split(separator: ":").last ?? Substring(image.digest))",
                "ParentId": "",
                "RepoTags": [image.reference],
                "RepoDigests": ["\(image.reference)@\(image.digest)"],
                "Created": 0,
                "Size": image.descriptor.size,
                "SharedSize": -1,
                "VirtualSize": image.descriptor.size,
                "Containers": -1,
                "Labels": [:],
            ] as [String: Any]
        }
        return try Self.json(.ok, results)
    }

    private func inspectImage(name: String) async throws -> DockerEngineResponse {
        let image = try await ClientImage.get(reference: name, containerSystemConfig: containerSystemConfig)
        let imageConfig = try await image.config(for: .current).config
        return try Self.json(.ok, [
            "Id": "sha256:\(image.digest.split(separator: ":").last ?? Substring(image.digest))",
            "RepoTags": [image.reference],
            "RepoDigests": ["\(image.reference)@\(image.digest)"],
            "Created": "1970-01-01T00:00:00Z",
            "Size": image.descriptor.size,
            "Architecture": Platform.current.architecture,
            "Os": "linux",
            "Config": [
                "Cmd": imageConfig?.cmd ?? [],
                "Entrypoint": imageConfig?.entrypoint ?? [],
                "Env": imageConfig?.env ?? [],
                "WorkingDir": imageConfig?.workingDir ?? "",
                "User": imageConfig?.user ?? "",
                "Labels": imageConfig?.labels ?? [:],
            ],
        ] as [String: Any])
    }

    private func createNetwork(body: Data) async throws -> DockerEngineResponse {
        let request = try JSONDecoder().decode(DockerNetworkCreateRequest.self, from: body)
        guard NetworkResource.nameValid(request.name) else {
            throw DockerAPIError.badRequest("ネットワーク名が正しくありません")
        }
        guard request.driver == nil || request.driver == "bridge" || request.driver == "container-network-vmnet" else {
            throw DockerAPIError.unsupported("Docker network driver \(request.driver ?? "") はサポートされていません")
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

    private static func containerSummary(_ snapshot: ContainerSnapshot) -> [String: Any] {
        let config = snapshot.configuration
        let process = config.initProcess
        let state = snapshot.status == .running ? "running" : (snapshot.startedDate == nil ? "created" : "exited")
        return [
            "Id": snapshot.id,
            "Names": ["/\(snapshot.id)"],
            "Image": config.image.reference,
            "ImageID": config.image.digest,
            "Command": ([process.executable] + process.arguments).joined(separator: " "),
            "Created": Int64(config.creationDate.timeIntervalSince1970),
            "Ports": publishedPortSummary(config.publishedPorts),
            "Labels": config.labels,
            "State": state,
            "Status": state,
            "HostConfig": ["NetworkMode": config.networks.first?.network ?? "default"],
            "NetworkSettings": ["Networks": networkSettings(snapshot.networks, configured: config.networks, baseDomain: config.dns?.domain ?? fallbackDNSDomain)],
        ]
    }

    private static func containerInspection(_ snapshot: ContainerSnapshot, runtime: DockerEngineExitState?) -> [String: Any] {
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
        return [
            "Id": snapshot.id,
            "Created": ISO8601DateFormatter().string(from: config.creationDate),
            "Path": process.executable,
            "Args": process.arguments,
            "State": ["Status": stateStatus, "Running": running, "Paused": false, "Restarting": false, "OOMKilled": false, "Dead": false, "Pid": 0, "ExitCode": exitCode, "Error": "", "StartedAt": startedAt, "FinishedAt": finishedAt],
            "Image": config.image.digest,
            "Name": "/\(snapshot.id)",
            "RestartCount": 0,
            "Driver": "containerization",
            "Platform": "linux",
            "MountLabel": "",
            "ProcessLabel": "",
            "AppArmorProfile": "",
            "ExecIDs": NSNull(),
            "HostConfig": [
                "NetworkMode": config.networks.first?.network ?? "default",
                "ReadonlyRootfs": config.readOnly,
                "PortBindings": publishedPortBindings(config.publishedPorts),
                "Binds": [],
            ],
            "GraphDriver": ["Name": "containerization", "Data": [:]],
            "Config": [
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
            ],
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
                "IPPrefixLen": attachment.ipv4Address.prefixLength,
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
        let decoded = try JSONDecoder().decode([String: [String]].self, from: Data(value.utf8))
        var labels = [String: String]()
        for label in decoded["label"] ?? [] {
            let parts = label.split(separator: "=", maxSplits: 1).map(String.init)
            if parts.count == 2 {
                labels[parts[0]] = "^\(NSRegularExpression.escapedPattern(for: parts[1]))$"
            } else {
                labels[label] = ".*"
            }
        }
        let statuses = decoded["status"] ?? []
        let status: RuntimeStatus? = statuses.contains("running") ? .running : (statuses.contains("exited") || statuses.contains("stopped") ? .stopped : nil)
        return ContainerListFilters(status: status, labels: labels)
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

        if let healthcheck = request["Healthcheck"], hasNonDefaultValue(healthcheck, field: "Healthcheck") {
            throw DockerAPIError.unsupported("Healthcheck はサポートされていません")
        }

        guard let hostConfig = request["HostConfig"] as? [String: Any] else { return }

        let supported: Set<String> = [
            "Binds", "Mounts", "PortBindings", "NetworkMode", "Memory", "NanoCpus",
            "ReadonlyRootfs", "ShmSize", "Dns", "DnsSearch", "DnsOptions", "Sysctls", "Tmpfs",
        ]
        for (key, value) in hostConfig where !supported.contains(key) && hasNonDefaultValue(value, field: key) {
            throw DockerAPIError.unsupported("HostConfig.\(key) が指定されていますが、この API ではサポートされていません")
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
        case hostConfig = "HostConfig"
        case networkingConfig = "NetworkingConfig"
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
    let shmSize: Int64?
    let dns: [String]?
    let dnsSearch: [String]?
    let dnsOptions: [String]?
    let sysctls: [String: String]?
    let tmpfs: [String: String]?

    init() {
        binds = nil
        mounts = nil
        portBindings = nil
        networkMode = nil
        memory = nil
        nanoCPUs = nil
        readonlyRootfs = nil
        shmSize = nil
        dns = nil
        dnsSearch = nil
        dnsOptions = nil
        sysctls = nil
        tmpfs = nil
    }

    enum CodingKeys: String, CodingKey {
        case binds = "Binds"
        case mounts = "Mounts"
        case portBindings = "PortBindings"
        case networkMode = "NetworkMode"
        case memory = "Memory"
        case nanoCPUs = "NanoCpus"
        case readonlyRootfs = "ReadonlyRootfs"
        case shmSize = "ShmSize"
        case dns = "Dns"
        case dnsSearch = "DnsSearch"
        case dnsOptions = "DnsOptions"
        case sysctls = "Sysctls"
        case tmpfs = "Tmpfs"
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

actor DockerEngineRuntimeState {
    private var exits = [String: DockerEngineExitState]()

    func started(id: String) {
        exits.removeValue(forKey: id)
    }

    func exited(id: String, exitCode: Int32) {
        exits[id] = DockerEngineExitState(exitCode: exitCode, finishedAt: Date())
    }

    func state(id: String) -> DockerEngineExitState? {
        exits[id]
    }

    func removed(id: String) {
        exits.removeValue(forKey: id)
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
