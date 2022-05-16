var consumerConfig = new ConsumerConfig
{
    BootstrapServers = Configuration.BootstrapServers,
    GroupId = "Consumer_" + Configuration.TopicCash,
    EnableAutoCommit = false
};
var socket = new PullSocket();
var cancellationTokenSource = new CancellationTokenSource();

Console.WriteLine("Start consuming message from " + Configuration.TopicCash);

OpenSocket();

StartConsumeTask();

PauseOrReleaseConsumerTask();

void PauseOrReleaseConsumerTask()
{
    while (true)
    {
        string replyStr = socket.ReceiveFrameString();
        if (!string.IsNullOrEmpty(replyStr))
        {
            if (replyStr.Contains(Configuration.PauseFlag))
            {
                cancellationTokenSource.Cancel();
                if (Configuration.HasAlreadyConsumerRunned)
                {
                    Console.WriteLine("\nOops, consuming message from Kafka is being paused");
                    Console.WriteLine("Waiting for \"release\" message...");
                    Configuration.HasAlreadyConsumerRunned = false;
                }
            }
            if (replyStr.Contains(Configuration.ReleaseFlag))
            {
                if (!Configuration.HasAlreadyConsumerRunned)
                {
                    cancellationTokenSource = new CancellationTokenSource();
                    StartConsumeTask();
                    Console.WriteLine("\nYeah, consuming message from Kafka is back");

                }
            }
        }
    }
}

void StartConsumeTask()
{
    Configuration.HasAlreadyConsumerRunned = true;
    ConsumeMessageTask(HandleMessage);
}

void ConsumeMessageTask(Action<ConsumeResult<Ignore, string>, IConsumer<Ignore, string>> action)
{
    Task.Factory.StartNew(() =>
    {
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Assign(new TopicPartition(Configuration.TopicCash, Configuration.PartitionId));
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string> consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                action(consumeResult, consumer);
            }
            consumer.Close();
        }
    }, TaskCreationOptions.LongRunning);
}

void HandleMessage(ConsumeResult<Ignore, string> consumeResult, IConsumer<Ignore, string> consumer)
{
    if (consumeResult != null)
    {
        Console.WriteLine(consumeResult.Message.Value + " - Offset: " + consumeResult.Offset);
        consumer.Commit(consumeResult);
    }
}

void OpenSocket()
{
    socket.Bind(Configuration.Host);
}

