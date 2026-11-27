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

typealias DockerHTTPChannel = NIOAsyncChannel<HTTPServerRequestPart, HTTPServerResponsePart>
typealias DockerHTTPWriter = NIOAsyncChannelOutboundWriter<HTTPServerResponsePart>

private final class DockerHTTPConnection: @unchecked Sendable {
    static let responseEncoderName = "DockerEngineHTTPResponseEncoder"
    static let requestDecoderName = "DockerEngineHTTPRequestDecoder"
    static let responseValidatorName = "DockerEngineHTTPResponseValidator"
    static let protocolErrorHandlerName = "DockerEngineHTTPProtocolErrorHandler"

    let channel: Channel
    let http: DockerHTTPChannel
    let rawStream: DockerEngineRawStreamHandler

    init(channel: Channel, http: DockerHTTPChannel, rawStream: DockerEngineRawStreamHandler) {
        self.channel = channel
        self.http = http
        self.rawStream = rawStream
    }

    func prepareRawStream(_ stream: DockerEngineExecStream) async throws {
        try await channel.pipeline.removeHandler(name: Self.requestDecoderName).get()
        rawStream.prepare(stream)
    }

    func activateRawStream() async throws {
        try await channel.pipeline.removeHandler(name: Self.responseEncoderName).get()
        try await channel.pipeline.removeHandler(name: Self.responseValidatorName).get()
        try await channel.pipeline.removeHandler(name: Self.protocolErrorHandlerName).get()
        rawStream.activate()
    }
}

final class DockerEngineRawStreamHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private var context: ChannelHandlerContext?
    private var stream: DockerEngineExecStream?
    private var isRawInput = false
    private var isRawOutput = false

    func handlerAdded(context: ChannelHandlerContext) {
        self.context = context
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard isRawInput else {
            context.fireChannelRead(data)
            return
        }
        let buffer = Self.unwrapInboundIn(data)
        stream?.receive(Data(buffer.readableBytesView))
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        guard isRawOutput else {
            context.write(data, promise: promise)
            return
        }
        context.write(Self.wrapOutboundOut(Self.unwrapOutboundIn(data)), promise: promise)
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case ChannelEvent.inputClosed = event, isRawInput {
            stream?.closeInput()
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        stream?.closeInput()
        context.fireChannelInactive()
    }

    func prepare(_ stream: DockerEngineExecStream) {
        context?.eventLoop.execute {
            self.stream = stream
            self.isRawInput = true
        }
    }

    func activate() {
        context?.eventLoop.execute {
            self.isRawOutput = true
            self.stream?.attach(to: self)
        }
    }

    func send(_ data: Data) {
        guard let eventLoop = context?.eventLoop else { return }
        eventLoop.execute { [weak self] in
            guard let self, let context = self.context else { return }
            var buffer = context.channel.allocator.buffer(capacity: data.count)
            buffer.writeBytes(data)
            context.writeAndFlush(Self.wrapOutboundOut(buffer), promise: nil)
        }
    }

    func close() {
        guard let eventLoop = context?.eventLoop else { return }
        eventLoop.execute { [weak self] in
            self?.context?.close(promise: nil)
        }
    }
}

final class DockerEngineExecStream: @unchecked Sendable {
    private static let maxOutputBytes = 8 * 1024 * 1024

    private let stdinPipe: Pipe?
    private let stdoutPipe = Pipe()
    private let stderrPipe = Pipe()
    private let tty: Bool
    private let attachStdout: Bool
    private let attachStderr: Bool
    private let lock = NSLock()
    private let inputQueue = DispatchQueue(label: "com.apple.container.docker-exec-stdin")
    private var transport: DockerEngineRawStreamHandler?
    private var capturedBytes = 0
    private var stdoutEnded = false
    private var stderrEnded = false
    private var processFinished = false

    init(tty: Bool, attachStdin: Bool, attachStdout: Bool, attachStderr: Bool) {
        self.tty = tty
        self.attachStdout = attachStdout
        self.attachStderr = attachStderr
        self.stdinPipe = attachStdin ? Pipe() : nil
    }

    var stdio: [FileHandle?] {
        [
            stdinPipe?.fileHandleForReading,
            (attachStdout || (tty && attachStderr)) ? stdoutPipe.fileHandleForWriting : nil,
            (!tty && attachStderr) ? stderrPipe.fileHandleForWriting : nil,
        ]
    }

    func closeOutputWriters() {
        try? stdoutPipe.fileHandleForWriting.close()
        try? stderrPipe.fileHandleForWriting.close()
    }

    func attach(to transport: DockerEngineRawStreamHandler) {
        lock.lock()
        self.transport = transport
        lock.unlock()
        stdoutPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            self?.readAvailable(from: handle, streamID: 1)
        }
        stderrPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            self?.readAvailable(from: handle, streamID: 2)
        }
    }

    func receive(_ data: Data) {
        guard let stdin = stdinPipe?.fileHandleForWriting else { return }
        inputQueue.async {
            try? stdin.write(contentsOf: data)
        }
    }

    func closeInput() {
        guard let stdin = stdinPipe?.fileHandleForWriting else { return }
        inputQueue.async {
            try? stdin.close()
        }
    }

    func finish() {
        lock.lock()
        processFinished = true
        lock.unlock()
        closeOutputWriters()
        closeInput()
        closeTransportIfFinished()
    }

    private func readAvailable(from handle: FileHandle, streamID: UInt8) {
        let chunk = handle.availableData
        guard !chunk.isEmpty else {
            handle.readabilityHandler = nil
            lock.lock()
            if streamID == 1 {
                stdoutEnded = true
            } else {
                stderrEnded = true
            }
            lock.unlock()
            closeTransportIfFinished()
            return
        }

        lock.lock()
        let remaining = Self.maxOutputBytes - capturedBytes
        if remaining > 0 {
            capturedBytes += min(chunk.count, remaining)
        }
        let transport = self.transport
        lock.unlock()
        guard remaining > 0, let transport else { return }
        let data = Data(chunk.prefix(remaining))
        transport.send(tty ? data : Self.multiplexed(data, streamID: streamID))
    }

    private func closeTransportIfFinished() {
        lock.lock()
        let shouldClose = processFinished && stdoutEnded && stderrEnded
        let transport = self.transport
        lock.unlock()
        if shouldClose {
            transport?.close()
        }
    }

    private static func multiplexed(_ data: Data, streamID: UInt8) -> Data {
        var frame = Data([streamID, 0, 0, 0])
        var length = UInt32(data.count).bigEndian
        withUnsafeBytes(of: &length) { frame.append(contentsOf: $0) }
        frame.append(data)
        return frame
    }
}

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
                    let responseEncoder = HTTPResponseEncoder()
                    let requestDecoder = ByteToMessageHandler(HTTPRequestDecoder())
                    let responseValidator = NIOHTTPResponseHeadersValidator()
                    let protocolErrorHandler = HTTPServerProtocolErrorHandler()
                    let rawStream = DockerEngineRawStreamHandler()
                    try channel.pipeline.syncOperations.addHandler(
                        responseEncoder,
                        name: DockerHTTPConnection.responseEncoderName
                    )
                    try channel.pipeline.syncOperations.addHandler(
                        requestDecoder,
                        name: DockerHTTPConnection.requestDecoderName
                    )
                    try channel.pipeline.syncOperations.addHandler(
                        responseValidator,
                        name: DockerHTTPConnection.responseValidatorName
                    )
                    try channel.pipeline.syncOperations.addHandler(
                        protocolErrorHandler,
                        name: DockerHTTPConnection.protocolErrorHandlerName
                    )
                    try channel.pipeline.syncOperations.addHandler(rawStream)
                    let http = try DockerHTTPChannel(wrappingChannelSynchronously: channel)
                    return DockerHTTPConnection(channel: channel, http: http, rawStream: rawStream)
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
            try await connection.http.executeThenClose { inbound, outbound in
                var head: HTTPRequestHead?
                var body = Data()
                var maxBodyBytes = maxDockerRequestBodyBytes
                var requestBodyBytes = 0
                var archiveRoot: URL?
                var archiveURL: URL?
                var archiveHandle: FileHandle?
                defer {
                    try? archiveHandle?.close()
                    if let archiveRoot {
                        try? FileManager.default.removeItem(at: archiveRoot)
                    }
                }
                for try await part in inbound {
                    switch part {
                    case .head(let requestHead):
                        try? archiveHandle?.close()
                        archiveHandle = nil
                        if let archiveRoot {
                            try? FileManager.default.removeItem(at: archiveRoot)
                        }
                        archiveRoot = nil
                        archiveURL = nil
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
                        if Self.isArchiveRequest(requestHead) {
                            let root = FileManager.default.temporaryDirectory
                                .appendingPathComponent("docker-engine-archive-\(UUID().uuidString)", isDirectory: true)
                            do {
                                try FileManager.default.createDirectory(
                                    at: root,
                                    withIntermediateDirectories: false,
                                    attributes: [.posixPermissions: 0o700]
                                )
                                let archive = root.appendingPathComponent("request.tar")
                                guard FileManager.default.createFile(
                                    atPath: archive.path,
                                    contents: nil,
                                    attributes: [.posixPermissions: 0o600]
                                ) else {
                                    throw CocoaError(.fileWriteUnknown)
                                }
                                archiveHandle = try FileHandle(forWritingTo: archive)
                                archiveRoot = root
                                archiveURL = archive
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
                            if let archiveHandle {
                                do {
                                    try archiveHandle.write(contentsOf: Data(bytes))
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
                            try? archiveHandle?.close()
                            archiveHandle = nil
                            let response = await router.handle(
                                method: requestHead.method,
                                uri: requestHead.uri,
                                headers: requestHead.headers,
                                body: body,
                                archiveURL: archiveURL
                            )
                            try await Self.write(response, outbound: outbound, connection: connection)
                        }
                        head = nil
                        body.removeAll(keepingCapacity: true)
                        requestBodyBytes = 0
                        if let archiveRoot {
                            try? FileManager.default.removeItem(at: archiveRoot)
                        }
                        archiveRoot = nil
                        archiveURL = nil
                    }
                }
            }
        } catch {
            log.debug("Docker API 接続を終了しました", metadata: ["error": "\(error)"])
        }
    }

    private static func maxRequestBodyBytes(for request: HTTPRequestHead) -> Int {
        isArchiveRequest(request) ? maxDockerBuildContextBytes : maxDockerRequestBodyBytes
    }

    private static func isArchiveRequest(_ request: HTTPRequestHead) -> Bool {
        guard let path = URLComponents(string: request.uri)?.path else { return false }
        let parts = path.split(separator: "/")
        let endpointParts = Array(
            parts.first?.range(of: #"^v\d+\.\d+$"#, options: .regularExpression) != nil
                ? parts.dropFirst()
                : parts[...]
        )
        return (request.method == .POST && endpointParts.elementsEqual(["build"]))
            || (request.method == .POST && endpointParts.elementsEqual(["images", "load"]))
            || (request.method == .PUT && endpointParts.count == 3 && endpointParts[0] == "containers" && endpointParts[2] == "archive")
    }

    private static func expectsContinue(_ request: HTTPRequestHead) -> Bool {
        request.headers["expect"].contains { value in
            value.split(separator: ",").contains { $0.trimmingCharacters(in: .whitespaces).lowercased() == "100-continue" }
        }
    }

    private static func write(
        _ response: DockerEngineResponse,
        outbound: DockerHTTPWriter,
        connection: DockerHTTPConnection? = nil
    ) async throws {
        var headers = HTTPHeaders()
        if response.status == .switchingProtocols {
            headers.add(name: "Connection", value: "Upgrade")
            headers.add(name: "Upgrade", value: "tcp")
            headers.add(name: "Content-Type", value: response.contentType)
        } else {
            if !response.body.isEmpty {
                headers.add(name: "Content-Type", value: response.contentType)
            }
            headers.add(name: "Content-Length", value: "\(response.body.count)")
        }
        headers.add(name: "API-Version", value: DockerEngineRouter.apiVersion)
        headers.add(name: "Docker-Experimental", value: "false")

        if let execStream = response.execStream {
            guard let connection else {
                throw ChannelError.inappropriateOperationForState
            }
            try await connection.prepareRawStream(execStream)
            try await outbound.write(.head(HTTPResponseHead(version: .http1_1, status: response.status, headers: headers)))
            try await outbound.write(.end(nil))
            try await connection.activateRawStream()
            return
        }

        try await outbound.write(.head(HTTPResponseHead(version: .http1_1, status: response.status, headers: headers)))
        if let bodyStream = response.bodyStream {
            for await chunk in bodyStream where !chunk.isEmpty {
                try await outbound.write(.body(.byteBuffer(ByteBuffer(data: chunk))))
            }
        } else if !response.body.isEmpty {
            try await outbound.write(.body(.byteBuffer(ByteBuffer(bytes: response.body))))
        }
        try await outbound.write(.end(nil))
    }
}

struct DockerEngineResponse: Sendable {
    let status: HTTPResponseStatus
    let body: Data
    let contentType: String
    let bodyStream: AsyncStream<Data>?
    let execStream: DockerEngineExecStream?

    init(
        status: HTTPResponseStatus,
        body: Data = Data(),
        contentType: String = "application/json",
        bodyStream: AsyncStream<Data>? = nil,
        execStream: DockerEngineExecStream? = nil
    ) {
        self.status = status
        self.body = body
        self.contentType = contentType
        self.bodyStream = bodyStream
        self.execStream = execStream
    }
}
