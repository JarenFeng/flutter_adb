import 'dart:async';
import 'dart:io';

import 'package:flutter_adb/adb_crypto.dart';
import 'package:flutter_adb/dart_socket_adb_transport.dart';

/// 不依赖 Flutter runner，直接通过 dart 运行关闭期间的迟到连接回归
Future<void> main() async {
  final opening = Completer<Socket>();
  final socket = _LateSocket();
  var attempts = 0;
  final transport = DartSocketAdbTransport(
    AdbCrypto(),
    socketConnector: (host, port, {timeout}) {
      attempts++;
      return opening.future;
    },
  );
  Object? connectFailure;
  final connecting = transport.connect('192.0.2.1', 5555).catchError((Object error) {
    connectFailure = error;
  });
  var closeCompleted = false;
  final closing = transport.close();
  final sameClosing = transport.close();
  _check(identical(closing, sameClosing), 'Concurrent close must share the same cleanup');
  final closed = closing.then((_) => closeCompleted = true);
  await Future<void>.delayed(Duration.zero);
  _check(!closeCompleted, 'Close must wait for the original pending connection');
  opening.complete(socket);
  await Future.wait([connecting, closed]);
  _check(connectFailure is StateError, 'A late connection must reject its original connect call');
  _check(!transport.connected, 'A late connection must not reactivate the transport');
  _check(socket.destroyCalls == 1, 'The precise late socket must be destroyed exactly once');
  _check(socket.optionCalls == 0, 'The late socket must never be configured or exposed');
  await transport.input.drain<void>();
  await transport.close();
  _check(socket.destroyCalls == 1, 'Repeated close must not destroy a successor resource');
  var rejectedReuse = false;
  try {
    await transport.connect('192.0.2.2', 5555);
  } on StateError {
    rejectedReuse = true;
  }
  _check(rejectedReuse && attempts == 1, 'A closed transport must not start another connection');
  print('PASS: close waits for and destroys the late socket without reactivation');
}

void _check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

final class _LateSocket implements Socket {
  int destroyCalls = 0;
  int optionCalls = 0;

  @override
  Future<void> flush() async {}

  @override
  void destroy() => destroyCalls++;

  @override
  bool setOption(SocketOption option, bool enabled) {
    optionCalls++;
    return true;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => throw StateError('Unexpected late socket operation: ${invocation.memberName}');
}
