Console.WriteLine("Start consuming message from " + Configuration.TopicCash);

OpenSocket();

StartConsumeTask();

PauseOrReleaseConsumerTask();

void PauseOrReleaseConsumerTask()
{
    while (true)
    {
        string replyStr = Configuration.Socket.ReceiveFrameString();
        if (!string.IsNullOrEmpty(replyStr))
        {
            if (replyStr.Contains(Configuration.PauseFlag))
            {
                Configuration.CancellationTokenSource.Cancel();
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
                    Configuration.CancellationTokenSource = new CancellationTokenSource();
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
        using (var consumer = new ConsumerBuilder<Ignore, string>(Configuration.ConsumerConfig).Build())
        {
            consumer.Assign(new TopicPartition(Configuration.TopicCash, 0));
            while (!Configuration.CancellationTokenSource.Token.IsCancellationRequested)
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
        Console.WriteLine(consumeResult.Message.Value + " - Thread: " + Thread.GetCurrentProcessorId());
        consumer.Commit(consumeResult);
    }
}

void OpenSocket()
{
    Configuration.Socket.Bind(Configuration.Host);
}

