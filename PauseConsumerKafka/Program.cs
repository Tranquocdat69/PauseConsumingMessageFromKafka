using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Core;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka.Configurations;
using NetMQ;
using NetMQ.Sockets;
using PauseConsumerKafka;

var socket = new PushSocket();
string host = "tcp://localhost:8888";
socket.Connect(host);

ConsumerBuilderConfiguration consumerConfig = new ConsumerBuilderConfiguration()
{
    BootstrapServers = "localhost:9092",
    EnableAutoCommit = false
};
ISubcriber<ConsumerData<string, string>> sucriber = new KafkaSubcriber<string, string>(consumerConfig);
var kafkaSubcriberService = new KafkaSubcriberService<string, string>(sucriber);

Console.WriteLine("Start consuming message from " + ConfigConsume.TopicB);
StartConsumeTopicB(kafkaSubcriberService, ConfigConsume.TopicB, 0, 0);
Console.ReadLine();

void StartConsumeTopicB(
           IKafkaSubcriberService<string, string> consumeService,
           string topic,
           long currentOffset,
           int partition)
{
    consumeService.StartConsumeTask(record =>
    {
        if (record != null)
        {
            if (record.Message.Value.Contains("pause"))
            {
                socket.SendFrame("pause");
                Console.WriteLine("Pause consuming message from " + ConfigConsume.TopicA);
            }
            if (record.Message.Value.Contains("release"))
            {
                socket.SendFrame("release");
                Console.WriteLine("Release consuming message from " + ConfigConsume.TopicA);
            }
        }
    }, topic, currentOffset, partition, default);
}
