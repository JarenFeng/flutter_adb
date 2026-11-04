import 'dart:async';
import 'dart:typed_data';

import 'package:flutter_adb/flutter_adb.dart';
import 'package:flutter_adb/adb_protocol.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('fixed package receives an immediate OPEN acknowledgement', () async {
    final transport = _ImmediateTransport();
    final connection = AdbConnection('test-host', 5555, AdbCrypto(), transportFactory: () => transport);
    expect(await connection.connect(), isTrue);
    try {
      final stream = await connection.openShell().timeout(const Duration(milliseconds: 100));
      expect(stream.remoteId, 7);
    } finally {
      await connection.disconnect();
    }
  });

  test('open waits for a later acknowledgement', () async {
    final transport = _ImmediateTransport(delayReady: true);
    final connection = AdbConnection('test-host', 5555, AdbCrypto(), transportFactory: () => transport);
    expect(await connection.connect(), isTrue);
    final opening = connection.openShell();
    await transport.opened.future;
    transport.acknowledge();
    expect((await opening.timeout(const Duration(milliseconds: 100))).remoteId, 7);
    await connection.disconnect();
    expect(connection.openStreams, isEmpty);
  });

  test('remote rejection completes promptly and removes the stream', () async {
    final transport = _ImmediateTransport(refuse: true);
    final connection = AdbConnection('test-host', 5555, AdbCrypto(), transportFactory: () => transport);
    expect(await connection.connect(), isTrue);
    await expectLater(connection.openShell().timeout(const Duration(milliseconds: 100)), throwsA(isA<Exception>().having((error) => error.toString(), 'message', contains('refused'))));
    expect(connection.openStreams, isEmpty);
    await connection.disconnect();
  });

  test('disconnect while opening completes promptly and cleans the stream', () async {
    final transport = _ImmediateTransport(delayReady: true);
    final connection = AdbConnection('test-host', 5555, AdbCrypto(), transportFactory: () => transport);
    expect(await connection.connect(), isTrue);
    final opening = expectLater(connection.openShell().timeout(const Duration(milliseconds: 100)), throwsA(isA<Exception>().having((error) => error.toString(), 'message', contains('refused'))));
    await transport.opened.future;
    await connection.disconnect();
    await opening;
    expect(connection.openStreams, isEmpty);
  });
}

class _ImmediateTransport implements AdbTransport {
  _ImmediateTransport({this.delayReady = false, this.refuse = false});
  final bool delayReady;
  final bool refuse;
  final opened = Completer<void>();
  int? _localId;

  void acknowledge() => _input.add(AdbProtocol.generateReady(7, _localId!));
  final _input = StreamController<Uint8List>();
  bool _connected = false;

  @override
  Stream<Uint8List> get input => _input.stream;
  @override
  bool get connected => _connected;
  @override
  Future<void> connect(String host, int port) async => _connected = true;
  @override
  Future<void> close() async {
    _connected = false;
    unawaited(_input.close());
  }

  @override
  Future<void> write(Uint8List data) async {
    final header = ByteData.sublistView(data);
    final command = header.getUint32(0, Endian.little);
    if (command == AdbProtocol.CMD_CNXN) {
      _input.add(AdbProtocol.generateConnect());
    } else if (command == AdbProtocol.CMD_OPEN) {
      _localId = header.getUint32(4, Endian.little);
      if (refuse) {
        _input.add(AdbProtocol.generateClose(7, _localId!));
      } else if (!delayReady) {
        acknowledge();
      }
      opened.complete();
    }
    await Future<void>.delayed(Duration.zero);
  }
}
