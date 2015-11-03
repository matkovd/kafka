library kafka.test.producer;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

main() {
  group('Producer', () {
    KafkaClient _client;
    String _topicName = 'dartKafkaTest';

    setUp(() async {
      var host = await getDefaultHost();
      _client = new KafkaClient([new KafkaHost(host, 9092)]);
    });

    tearDown(() async {
      await _client.close();
    });

    test('it can produce messages to multiple brokers', () async {
      var producer = new Producer(_client, 1, 100);
      producer.addMessages(_topicName, 0, [new Message('test1'.codeUnits)]);
      producer.addMessages(_topicName, 1, [new Message('test2'.codeUnits)]);
      producer.addMessages(_topicName, 2, [new Message('test3'.codeUnits)]);
      var result = await producer.send();
      expect(result.hasErrors, isFalse);
    });
  });
}