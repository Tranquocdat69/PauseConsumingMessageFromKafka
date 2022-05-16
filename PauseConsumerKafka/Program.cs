var consumerConfig = new ConsumerConfig
{
    BootstrapServers = Configuration.BootstrapServers,
    GroupId = "Consumer_" + Configuration.TopicFlag,
    EnableAutoCommit = false
};
var socket = new PushSocket();
var cancellationTokenSource = new CancellationTokenSource();

Console.WriteLine("Start consuming message from " + Configuration.TopicFlag);

ConnectSocket();

StartConsumeTask(HandleMessage);

Console.ReadLine();

void StartConsumeTask(Action<ConsumeResult<Ignore, string>, IConsumer<Ignore, string>> action)
{
    using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
    {
        consumer.Assign(new TopicPartition(Configuration.TopicFlag, 0));
        while (!cancellationTokenSource.Token.IsCancellationRequested)
        {
            ConsumeResult<Ignore, string> consumeResult = consumer.Consume();
            action(consumeResult, consumer);
        }
        consumer.Close();
    }
}

void HandleMessage(ConsumeResult<Ignore, string> consumeResult, IConsumer<Ignore, string> consumer)
{
    if (consumeResult != null)
    {
        if (consumeResult.Message.Value.Contains(Configuration.PauseFlag))
        {
            socket.SendFrame(Configuration.PauseFlag);
            Console.WriteLine("Pause consuming message from " + Configuration.TopicCash);
        }
        if (consumeResult.Message.Value.Contains(Configuration.ReleaseFlag))
        {
            socket.SendFrame(Configuration.ReleaseFlag);
            Console.WriteLine("Release consuming message from " + Configuration.TopicCash);
        }
        consumer.Commit(consumeResult);
    }
}

void ConnectSocket()
{
    socket.Connect(Configuration.Host);
}
