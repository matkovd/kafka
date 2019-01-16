library kafka.test.consumer_group;

import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import 'setup.dart';

void main() {
  group('ConsumerGroup:', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';
    Broker _coordinator;
    Broker _badCoordinator;

    setUp(() async {
      var host = await getDefaultHost();
      var session = new KafkaSession([new ContactPoint(host, 9092)]);
      var brokersMetadata = await session.getMetadata([_topicName].toSet());

      var metadata = await session.getConsumerMetadata('testGroup');
      _coordinator = metadata.coordinator;
      _badCoordinator =
          brokersMetadata.brokers.firstWhere((b) => b.id != _coordinator.id);
      _session = new DelegateKafkaSessionMock(session);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches offsets', () async {
      var group = new ConsumerGroup(_session, 'testGroup');
      var offsets = await group.fetchOffsets({
        _topicName: [0, 1, 2].toSet()
      });
      expect(offsets.length, equals(3));
      offsets.forEach((o) {
        expect(o.errorCode, 0);
      });
    });

    test('it tries to refresh coordinator host 3 times on fetchOffsets',
        () async {
      var _tempSession = KafkaSessionMock();
      when(_tempSession.getConsumerMetadata(any))
          .thenThrow(KafkaServerError(16));

      var group = new ConsumerGroup(_tempSession, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2].toSet()
        });
      } catch (e) {
        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(16));
      }
    });

    test(
        'it retries to fetchOffsets 3 times if it gets OffsetLoadInProgress error',
        () async {
      var _tempSession = KafkaSessionMock();

      var badOffsets = [
        new ConsumerOffset(_topicName, 0, -1, '', 14),
        new ConsumerOffset(_topicName, 1, -1, '', 14),
        new ConsumerOffset(_topicName, 2, -1, '', 14)
      ];
      when(_tempSession.getConsumerMetadata(any))
          .thenThrow(KafkaServerError(14));

      var group = new ConsumerGroup(_tempSession, 'testGroup');
      // Can't use expect(throws) here since it's async, so `verify` check below
      // fails.
      var now = new DateTime.now();
      try {
        await group.fetchOffsets({
          _topicName: [0, 1, 2].toSet()
        });
        fail('fetchOffsets must throw an error.');
      } catch (e) {
        var diff = now.difference(new DateTime.now());
        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(14));
      }
    });

    test('it tries to refresh coordinator host 3 times on commitOffsets',
        () async {
      var _tempSession = KafkaSessionMock();
      when(_tempSession.getConsumerMetadata(any))
          .thenThrow(KafkaServerError(16));

      var group = new ConsumerGroup(_tempSession, 'testGroup');
      var offsets = [new ConsumerOffset(_topicName, 0, 3, '')];

      try {
        await group.commitOffsets(offsets, -1, '');
      } catch (e) {
        expect(e, new isInstanceOf<KafkaServerError>());
        expect(e.code, equals(16));
      }
    });

    test('it can reset offsets to earliest', () async {
      var host = await getDefaultHost();
      var session = new KafkaSession([new ContactPoint(host, 9092)]);
      var offsetMaster = new OffsetMaster(session);
      var earliestOffsets = await offsetMaster.fetchEarliest({
        _topicName: [0, 1, 2].toSet()
      });

      var group = new ConsumerGroup(session, 'testGroup');
      await group.resetOffsetsToEarliest({
        _topicName: [0, 1, 2].toSet()
      });

      var offsets = await group.fetchOffsets({
        _topicName: [0, 1, 2].toSet()
      });
      expect(offsets, hasLength(3));

      for (var o in offsets) {
        var earliest =
            earliestOffsets.firstWhere((to) => to.partitionId == o.partitionId);
        expect(o.offset, equals(earliest.offset - 1));
      }
    });
  });
}

class KafkaSessionMock extends Mock implements KafkaSession {}

class DelegateKafkaSessionMock extends Mock implements KafkaSession {
  final KafkaSession _delegate;

  DelegateKafkaSessionMock(this._delegate);

  @override
  Future<Set<String>> listTopics() async {
    return _delegate.listTopics();
  }

  @override
  Future close() async {
    return _delegate.close();
  }

  @override
  Future<dynamic> send(Broker broker, KafkaRequest request) {
    return _delegate.send(broker, request);
  }

  @override
  Future<GroupCoordinatorResponse> getConsumerMetadata(
      String consumerGroup) async {
    return _delegate.getConsumerMetadata(consumerGroup);
  }
}
