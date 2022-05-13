Console.WriteLine("Start consuming message from " + Configuration.TopicB);

ConnectSocket();

StartConsumeTopicUsingConfluentKafa(Configuration.TopicB, Configuration.CancellationTokenSource.Token);

Console.ReadLine();

void StartConsumeTopicUsingConfluentKafa(string topic, CancellationToken cancellationToken)
{
    using (var consumer = new ConsumerBuilder<Ignore, string>(Configuration.ConsumerConfig).Build())
    {
        consumer.Assign(new TopicPartition(topic, 0));
        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<Ignore, string> record = consumer.Consume();
            if (record != null)
            {
                if (record.Message.Value.Contains(Configuration.PauseFlag))
                {
                    Configuration.Socket.SendFrame(Configuration.PauseFlag);
                    Console.WriteLine("Pause consuming message from " + Configuration.TopicA);
                }
                if (record.Message.Value.Contains(Configuration.ReleaseFlag))
                {
                    Configuration.Socket.SendFrame(Configuration.ReleaseFlag);
                    Console.WriteLine("Release consuming message from " + Configuration.TopicA);
                }
                consumer.Commit(record);
            }
        }
        consumer.Close();
    }
}

void ConnectSocket()
{
    Configuration.Socket.Connect(Configuration.Host);
}
