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

import ContainerPersistence
import Foundation
import Logging
import NIOCore
import NIOHTTP1
import NIOPosix

typealias DockerHTTPConnection = NIOAsyncChannel<HTTPServerRequestPart, HTTPServerResponsePart>
typealias DockerHTTPWriter = NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>

private let maxDockerRequestBodyBytes = 16 * 1024 * 1024
private let maxDockerBuildContextBytes = 1024 * 1024 * 1024

public struct DockerEngineListener: Sendable {
    fileprivate let channel: NIOAsyncChannel<DockerHTTPConnection, Never>
}

/// ローカル CLI からの接続を保護するため、0600 の Unix ソケットだけを bind する。
public struct DockerEngineServer: Sendable {
    private let socketPath: String
    private let containerSystemConfig: ContainerSystemConfig
    private let log: Logger
    private let runtimeState = DockerEngineRuntimeState()

    public init(socketPath: String, containerSystemConfig: ContainerSystemConfig, log: Logger) {
        self.socketPath = socketPath
        self.containerSystemConfig = containerSystemConfig
        self.log = log
    }

    public func run() async throws {
        let listener = try await bind()
        try await run(listener: listener)
    }

    public func bind() async throws -> DockerEngineListener {
        DockerEngineListener(channel: try await Self.bind(socketPath: socketPath))
    }

    public func run(listener: DockerEngineListener) async throws {
        defer { try? FileManager.default.removeItem(atPath: socketPath) }
        log.info("Docker Engine API を待ち受けています", metadata: ["socket": "\(socketPath)"])
        try await withThrowingDiscardingTaskGroup { group in
            try await listener.channel.executeThenClose { connections in
                for try await connection in connections {
                    group.addTask {
                        await self.serve(connection)
                    }
                }
            }
        }
    }

    private static func bind(socketPath: String) async throws -> NIOAsyncChannel<DockerHTTPConnection, Never> {
        let previousUmask = umask(0o177)
        defer { umask(previousUmask) }
        let server = try await ServerBootstrap(group: NIOSingletons.posixEventLoopGroup)
            .bind(unixDomainSocketPath: socketPath, cleanupExistingSocketFile: true) { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.configureHTTPServerPipeline(withPipeliningAssistance: false)
                    return try DockerHTTPConnection(wrappingChannelSynchronously: channel)
                }
            }
        guard chmod(socketPath, 0o600) == 0 else {
            throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
        }
        return server
    }

    private func serve(_ connection: DockerHTTPConnection) async {
        let router = DockerEngineRouter(containerSystemConfig: containerSystemConfig, log: log, runtimeState: runtimeState)
        do {
            try await connection.executeThenClose { inbound, outbound in
                var head: HTTPRequestHead?
                var body = Data()
                var maxBodyBytes = maxDockerRequestBodyBytes
                var requestBodyBytes = 0
                var buildArchiveRoot: URL?
                var buildArchiveURL: URL?
                var buildArchiveHandle: FileHandle?
                defer {
                    try? buildArchiveHandle?.close()
                    if let buildArchiveRoot {
                        try? FileManager.default.removeItem(at: buildArchiveRoot)
                    }
                }
                for try await part in inbound {
                    switch part {
                    case .head(let requestHead):
                        try? buildArchiveHandle?.close()
                        buildArchiveHandle = nil
                        if let buildArchiveRoot {
                            try? FileManager.default.removeItem(at: buildArchiveRoot)
                        }
                        buildArchiveRoot = nil
                        buildArchiveURL = nil
                        head = requestHead
                        body.removeAll(keepingCapacity: true)
                        requestBodyBytes = 0
                        maxBodyBytes = Self.maxRequestBodyBytes(for: requestHead)
                        if let contentLength = requestHead.headers.first(name: "content-length").flatMap({ Int64($0) }), contentLength > Int64(maxBodyBytes) {
                            try await Self.write(DockerEngineResponse(status: .payloadTooLarge), outbound: outbound)
                            connection.channel.close(promise: nil)
                            return
                        }
                        if Self.expectsContinue(requestHead) {
                            try await outbound.write(.head(HTTPResponseHead(version: requestHead.version, status: .continue)))
                            try await outbound.write(.end(nil))
                        }
                        if Self.isBuildRequest(requestHead) {
                            let root = FileManager.default.temporaryDirectory
                                .appendingPathComponent("docker-engine-build-\(UUID().uuidString)", isDirectory: true)
                            do {
                                try FileManager.default.createDirectory(
                                    at: root,
                                    withIntermediateDirectories: false,
                                    attributes: [.posixPermissions: 0o700]
                                )
                                let archive = root.appendingPathComponent("context.tar")
                                guard FileManager.default.createFile(
                                    atPath: archive.path,
                                    contents: nil,
                                    attributes: [.posixPermissions: 0o600]
                                ) else {
                                    throw CocoaError(.fileWriteUnknown)
                                }
                                buildArchiveHandle = try FileHandle(forWritingTo: archive)
                                buildArchiveRoot = root
                                buildArchiveURL = archive
                            } catch {
                                try? FileManager.default.removeItem(at: root)
                                try await Self.write(DockerEngineResponse(status: .internalServerError), outbound: outbound)
                                connection.channel.close(promise: nil)
                                return
                            }
                        }
                    case .body(let buffer):
                        guard requestBodyBytes + buffer.readableBytes <= maxBodyBytes else {
                            try await Self.write(DockerEngineResponse(status: .payloadTooLarge), outbound: outbound)
                            connection.channel.close(promise: nil)
                            return
                        }
                        requestBodyBytes += buffer.readableBytes
                        if let bytes = buffer.getBytes(at: buffer.readerIndex, length: buffer.readableBytes) {
                            if let buildArchiveHandle {
                                do {
                                    try buildArchiveHandle.write(contentsOf: Data(bytes))
                                } catch {
                                    try await Self.write(DockerEngineResponse(status: .internalServerError), outbound: outbound)
                                    connection.channel.close(promise: nil)
                                    return
                                }
                            } else {
                                body.append(contentsOf: bytes)
                            }
                        }
                    case .end:
                        if let requestHead = head {
                            try? buildArchiveHandle?.close()
                            buildArchiveHandle = nil
                            let response = await router.handle(
                                method: requestHead.method,
                                uri: requestHead.uri,
                                headers: requestHead.headers,
                                body: body,
                                buildArchive: buildArchiveURL
                            )
                            try await Self.write(response, outbound: outbound)
                        }
                        head = nil
                        body.removeAll(keepingCapacity: true)
                        requestBodyBytes = 0
                        if let buildArchiveRoot {
                            try? FileManager.default.removeItem(at: buildArchiveRoot)
                        }
                        buildArchiveRoot = nil
                        buildArchiveURL = nil
                    }
                }
            }
        } catch {
            log.debug("Docker API 接続を終了しました", metadata: ["error": "\(error)"])
        }
    }

    private static func maxRequestBodyBytes(for request: HTTPRequestHead) -> Int {
        isBuildRequest(request) ? maxDockerBuildContextBytes : maxDockerRequestBodyBytes
    }

    private static func isBuildRequest(_ request: HTTPRequestHead) -> Bool {
        guard request.method == .POST, let path = URLComponents(string: request.uri)?.path else { return false }
        let parts = path.split(separator: "/")
        let endpointParts = parts.first?.range(of: #"^v\d+\.\d+$"#, options: .regularExpression) != nil
            ? parts.dropFirst()
            : parts[...]
        return endpointParts.elementsEqual(["build"])
    }

    private static func expectsContinue(_ request: HTTPRequestHead) -> Bool {
        request.headers["expect"].contains { value in
            value.split(separator: ",").contains { $0.trimmingCharacters(in: .whitespaces).lowercased() == "100-continue" }
        }
    }

    private static func write(_ response: DockerEngineResponse, outbound: DockerHTTPWriter) async throws {
        var headers = HTTPHeaders()
        if !response.body.isEmpty {
            headers.add(name: "Content-Type", value: response.contentType)
        }
        headers.add(name: "Content-Length", value: "\(response.body.count)")
        headers.add(name: "API-Version", value: DockerEngineRouter.apiVersion)
        headers.add(name: "Docker-Experimental", value: "false")
        try await outbound.write(.head(HTTPResponseHead(version: .http1_1, status: response.status, headers: headers)))
        if !response.body.isEmpty {
            try await outbound.write(.body(.byteBuffer(ByteBuffer(bytes: response.body))))
        }
        try await outbound.write(.end(nil))
    }
}

struct DockerEngineResponse: Sendable {
    let status: HTTPResponseStatus
    let body: Data
    let contentType: String

    init(status: HTTPResponseStatus, body: Data = Data(), contentType: String = "application/json") {
        self.status = status
        self.body = body
        self.contentType = contentType
    }
}
