using ConsumerKafka;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Core;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka;
using FPTS.FIT.BDRD.BuildingBlocks.EventBus.Kafka.Configurations;
using NetMQ;
using NetMQ.Sockets;

var cts = new CancellationTokenSource();
var socket = new PullSocket();
string host = "tcp://localhost:8888";
const string PauseFlag = "pause";
const string ReleaseFlag = "release";

var consumerConfig = new ConsumerBuilderConfiguration()
{
    BootstrapServers = "localhost:9092",
    EnableAutoCommit = false
};
var sucriber = new KafkaSubcriber<string, string>(consumerConfig);
var kafkaSubcriberService = new KafkaSubcriberService<string, string>(sucriber);

Console.WriteLine("Start consuming message from " + ConfigConsume.TopicA);

OpenSocket();
StartConsumeTopicA();

while (true)
{
    string replyStr = socket.ReceiveFrameString();
    if (!string.IsNullOrEmpty(replyStr))
    {
        if (replyStr.Contains(PauseFlag))
        {
            cts.Cancel();
        }
        if (replyStr.Contains(ReleaseFlag))
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

void OpenSocket()
{
    socket.Bind(host);
}

