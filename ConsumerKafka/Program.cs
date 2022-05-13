Console.WriteLine("Start consuming message from " + Configuration.TopicA);

StartConsumeTask();

OpenSocket();

RunContinuousTask();

void RunContinuousTask()
{
    while (true)
    {
        string replyStr = Configuration.Socket.ReceiveFrameString();
        if (!string.IsNullOrEmpty(replyStr))
        {
            if (replyStr.Contains(Configuration.PauseFlag))
            {
                Configuration.CancellationTokenSource.Cancel();
                Configuration.HasAlreadyConsumerRunned = false;
            }
            if (replyStr.Contains(Configuration.ReleaseFlag))
            {
                if (!Configuration.HasAlreadyConsumerRunned)
                {
                    Configuration.CancellationTokenSource = new CancellationTokenSource();
                    StartConsumeTask();
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
            consumer.Assign(new TopicPartition(Configuration.TopicA, 0));
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

