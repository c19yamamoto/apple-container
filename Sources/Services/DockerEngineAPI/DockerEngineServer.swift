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
                    try channel.pipeline.syncOperations.configureHTTPServerPipeline()
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
                for try await part in inbound {
                    switch part {
                    case .head(let requestHead):
                        head = requestHead
                        body.removeAll(keepingCapacity: true)
                    case .body(let buffer):
                        guard body.count + buffer.readableBytes <= 16 * 1024 * 1024 else {
                            try await Self.write(DockerEngineResponse(status: .payloadTooLarge), outbound: outbound)
                            connection.channel.close(promise: nil)
                            return
                        }
                        if let bytes = buffer.getBytes(at: buffer.readerIndex, length: buffer.readableBytes) {
                            body.append(contentsOf: bytes)
                        }
                    case .end:
                        if let requestHead = head {
                            let response = await router.handle(
                                method: requestHead.method,
                                uri: requestHead.uri,
                                headers: requestHead.headers,
                                body: body
                            )
                            try await Self.write(response, outbound: outbound)
                        }
                        head = nil
                        body.removeAll(keepingCapacity: true)
                    }
                }
            }
        } catch {
            log.debug("Docker API 接続を終了しました", metadata: ["error": "\(error)"])
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
