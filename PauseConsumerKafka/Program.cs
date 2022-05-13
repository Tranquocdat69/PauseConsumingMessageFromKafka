Console.WriteLine("Start consuming message from " + Configuration.TopicB);

ConnectSocket();

StartConsumeTask(HandleMessage);

Console.ReadLine();

void StartConsumeTask(Action<ConsumeResult<Ignore, string>, IConsumer<Ignore, string>> action)
{
    using (var consumer = new ConsumerBuilder<Ignore, string>(Configuration.ConsumerConfig).Build())
    {
        consumer.Assign(new TopicPartition(Configuration.TopicB, 0));
        while (!Configuration.CancellationTokenSource.Token.IsCancellationRequested)
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
            Configuration.Socket.SendFrame(Configuration.PauseFlag);
            Console.WriteLine("Pause consuming message from " + Configuration.TopicA);
        }
        if (consumeResult.Message.Value.Contains(Configuration.ReleaseFlag))
        {
            Configuration.Socket.SendFrame(Configuration.ReleaseFlag);
            Console.WriteLine("Release consuming message from " + Configuration.TopicA);
        }
        consumer.Commit(consumeResult);
    }
}


void ConnectSocket()
{
    Configuration.Socket.Connect(Configuration.Host);
}
