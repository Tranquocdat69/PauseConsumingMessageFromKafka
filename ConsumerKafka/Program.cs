Console.WriteLine("Start consuming message from " + Configuration.TopicA);

Task.Factory.StartNew(() =>
{
    StartConsumeTopic(Configuration.TopicA);
}, TaskCreationOptions.LongRunning);

OpenSocket();

while (true)
{
    string replyStr = Configuration.Socket.ReceiveFrameString();
    if (!string.IsNullOrEmpty(replyStr))
    {
        if (replyStr.Contains(Configuration.PauseFlag))
        {
            Configuration.CancellationTokenSource.Cancel();
        }
        if (replyStr.Contains(Configuration.ReleaseFlag))
        {
            Configuration.CancellationTokenSource = new CancellationTokenSource();
            Task.Factory.StartNew(() =>
            {
                StartConsumeTopic(Configuration.TopicA);
            }, TaskCreationOptions.LongRunning);
        }
    }
}

void StartConsumeTopic(string topic)
{
    using (var consumer = new ConsumerBuilder<Ignore, string>(Configuration.ConsumerConfig).Build())
    {
        consumer.Assign(new TopicPartition(topic, 0));
        while (!Configuration.CancellationTokenSource.Token.IsCancellationRequested)
        {
            ConsumeResult<Ignore, string> record = consumer.Consume(TimeSpan.FromSeconds(1));
            if (record != null)
            {
                Console.WriteLine(record.Message.Value);
                consumer.Commit(record);
            }
        }
        consumer.Close();
    }
}

void OpenSocket()
{
    Configuration.Socket.Bind(Configuration.Host);
}

