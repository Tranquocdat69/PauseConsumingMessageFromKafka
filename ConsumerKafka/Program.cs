using ConsumerKafka;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Core;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka.Configurations;
using NetMQ;
using NetMQ.Sockets;

var socket = new PullSocket();
string host = "tcp://localhost:8888";
socket.Bind(host);

ConsumerBuilderConfiguration consumerConfig = new ConsumerBuilderConfiguration()
{
    BootstrapServers = "localhost:9092",
    EnableAutoCommit = false
};
ISubcriber<ConsumerData<string, string>> sucriber = new KafkaSubcriber<string, string>(consumerConfig);
var kafkaSubcriberService = new KafkaSubcriberService<string, string>(sucriber);

Console.WriteLine("Start consuming message from " + ConfigConsume.TopicA);
CancellationTokenSource cts = new CancellationTokenSource();
StartConsumeTopicA();

while (true)
{
    string replyStr = socket.ReceiveFrameString();
    if (!string.IsNullOrEmpty(replyStr))
    {
        if (replyStr.Contains("pause"))
        {
            cts.Cancel();
        }
        if (replyStr.Contains("release"))
        {
            cts = new CancellationTokenSource();
            StartConsumeTopicA();
        }
    }
}

void StartConsumeTopicA()
{
    kafkaSubcriberService.StartConsumeTask(record =>
    {
        if (record != null)
        {
            KafkaOffset.CurrentOffset = record.Offset;
            Console.WriteLine(record.Message.Value);
        }
    }, ConfigConsume.TopicA, KafkaOffset.CurrentOffset + 1, 0, cts.Token);
}

