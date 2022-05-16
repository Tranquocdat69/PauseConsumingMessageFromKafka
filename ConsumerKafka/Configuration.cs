namespace ConsumerKafka;
public class Configuration
{
    public const string BootstrapServers = "localhost:9092";

    public static string TopicCash = "TopicCash";

    public static string TopicFlag = "TopicFlag";

    public const int PartitionId = 0;

    public static string Host = "tcp://localhost:8888";

    public const string PauseFlag = "pause";

    public const string ReleaseFlag = "release";

    public static bool HasAlreadyConsumerRunned = false;
}