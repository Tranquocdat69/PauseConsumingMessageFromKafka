using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace PublishMessageToKafka.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublishMessageToKafka : ControllerBase
    {
        private readonly IProducer<Ignore, string> _producer;
        private const string TopicFlag = "TopicFlag";
        private const string TopicCash = "TopicCash";
        private const int PartitionId = 0;
        private const string PauseFlag = "pause";
        private const string ReleaseFlag = "release";

        public PublishMessageToKafka()
        {
            ProducerConfig producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };
            _producer = new ProducerBuilder<Ignore, string>(producerConfig).Build();
        }

        [HttpPost]
        [Route("PublishPauseMessageToTopicFlag")]
        public IActionResult PublishPauseMessageToTopicFlag()
        {
            ProduceMessage(new Message<Ignore, string>() { Value = PauseFlag }, TopicFlag, PartitionId); ;
            return Ok();
        }

        [HttpPost]
        [Route("PublishReleaseMessageToTopicFlag")]
        public IActionResult PublishReleaseMessageToTopicFlag()
        {
            ProduceMessage(new Message<Ignore, string>() { Value = ReleaseFlag }, TopicFlag, PartitionId); ;
            return Ok();
        }

        [HttpPost]
        [Route("PublishMessageToTopicCash{amountOfMessage}")]
        public IActionResult PublishMessageToTopicCash(int amountOfMessage)
        {
            for (int i = 0; i < amountOfMessage; i++)
            {
                ProduceMessage(new Message<Ignore, string>() { Value = "message " + i }, TopicCash, PartitionId); ;
            }

            return Ok();
        }

        private void ProduceMessage(Message<Ignore, string> message, string topic, int partiton)
        {
            try
            {
                if (partiton < 0)
                {
                    _producer.Produce(topic, message);
                }
                else
                {
                    _producer.Produce(new TopicPartition(topic, partiton), message);
                }
            }
            catch (ProduceException<Null, string> e)
            {
                if (e.Error.Code == ErrorCode.Local_QueueFull)
                {
                    _producer.Poll(TimeSpan.FromSeconds(1));
                }
                else
                {
                    throw;
                }
            }
        }
    }
}
