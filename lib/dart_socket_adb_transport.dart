// Copyright 2026 Jaren. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_adb/adb_certificate.dart';
import 'package:flutter_adb/adb_crypto.dart';
import 'package:flutter_adb/adb_transport.dart';

typedef DartSocketConnector = Future<Socket> Function(InternetAddress host, int port, {Duration? timeout});

/// The default [AdbTransport], backed by a Dart [Socket].
final class DartSocketAdbTransport implements AdbTlsTransport {
  DartSocketAdbTransport(this.crypto, {this.connectionTimeout = const Duration(seconds: 1), DartSocketConnector? socketConnector}) : _socketConnector = socketConnector ?? Socket.connect;

  final AdbCrypto crypto;
  final Duration connectionTimeout;
  final DartSocketConnector _socketConnector;

  Socket? _socket;
  Future<Socket>? _opening;
  Future<SecureSocket>? _upgrading;
  Future<void>? _closing;
  StreamController<Uint8List>? _inputController;
  StreamSubscription<Uint8List>? _socketSubscription;
  bool _connected = false;
  bool _started = false;
  bool _closed = false;

  @override
  Stream<Uint8List> get input => _inputController?.stream ?? const Stream<Uint8List>.empty();

  @override
  bool get connected => _connected && !_closed;

  @override
  Future<void> connect(String host, int port) async {
    if (_started || _closed) throw StateError('Transport cannot be reused');
    _started = true;
    final inputController = StreamController<Uint8List>();
    _inputController = inputController;
    try {
      final opening = _opening = _socketConnector(InternetAddress(host), port, timeout: connectionTimeout);
      final socket = await opening;
      if (_closed) {
        await close();
        throw StateError('Transport was closed during connect');
      }
      _socket = socket;
      socket.setOption(SocketOption.tcpNoDelay, true);
      _connected = true;
      _listenTo(socket, inputController);
    } catch (_) {
      await close();
      rethrow;
    }
  }

  void _listenTo(Socket socket, StreamController<Uint8List> inputController) {
    _socketSubscription = socket.listen(
      (data) {
        if (!_closed) inputController.add(data);
      },
      onError: (Object error, StackTrace stackTrace) {
        if (_closed) return;
        _connected = false;
        inputController.addError(error, stackTrace);
        unawaited(inputController.close());
      },
      onDone: () {
        if (_closed) return;
        _connected = false;
        unawaited(inputController.close());
      },
    );
  }

  @override
  Future<void> write(Uint8List data) async {
    final socket = _socket;
    if (!connected || socket == null) throw StateError('Transport is not connected');
    socket.add(data);
    await socket.flush();
  }

  @override
  Future<void> upgradeToTls() async {
    final socket = _socket;
    final inputController = _inputController;
    if (!connected || socket == null || inputController == null || _upgrading != null) throw StateError('Transport is not connected or TLS is already active');
    _socketSubscription?.pause();
    final securityContext = AdbCertificate.createTransportSecurityContext(crypto.keyPair);
    try {
      final upgrading = _upgrading = SecureSocket.secure(socket, context: securityContext, onBadCertificate: (_) => true);
      final secureSocket = await upgrading;
      if (_closed) {
        await close();
        throw StateError('Transport was closed during TLS upgrade');
      }
      _socket = secureSocket;
      _listenTo(secureSocket, inputController);
    } catch (_) {
      _connected = false;
      await close();
      rethrow;
    }
  }

  @override
  Future<void> close() {
    _closed = true;
    _connected = false;
    return _closing ??= _closeOwnedSocket();
  }

  Future<void> _closeOwnedSocket() async {
    // 等待原始连接和 TLS 结果，迟到的 socket 仍由本实例关闭
    if (_opening != null) {
      try {
        _socket ??= await _opening!;
      } catch (_) {}
    }
    if (_upgrading != null) {
      try {
        _socket = await _upgrading!;
      } catch (_) {}
    }
    final subscription = _socketSubscription;
    _socketSubscription = null;
    final socket = _socket;
    _socket = null;
    try {
      await subscription?.cancel();
    } finally {
      try {
        if (socket != null) {
          try {
            await socket.flush();
          } catch (_) {}
          socket.destroy();
        }
      } finally {
        final inputController = _inputController;
        _inputController = null;
        _opening = null;
        _upgrading = null;
        // 无订阅者时 close Future 会等待订阅，socket 清理不依赖消费者读取
        if (inputController != null && !inputController.isClosed) unawaited(inputController.close());
      }
    }
  }
}
