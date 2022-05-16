using Confluent.Kafka;
using ConsumerKafka;
using Microsoft.AspNetCore.Mvc;

namespace PublishMessageToKafka.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublishMessageToKafka : ControllerBase
    {
        private readonly IProducer<Null, string> _producer;

        public PublishMessageToKafka()
        {
            ProducerConfig producerConfig = new ProducerConfig
            {
                BootstrapServers = Configuration.BootstrapServers,
            };
            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        [HttpPost]
        [Route("PublishPauseMessageToTopicFlag")]
        public IActionResult PublishPauseMessageToTopicFlag()
        {
            ProduceMessage(new Message<Null, string>() { Value = Configuration.PauseFlag }, Configuration.TopicFlag, Configuration.PartitionId); ;
            return Ok();
        }

        [HttpPost]
        [Route("PublishReleaseMessageToTopicFlag")]
        public IActionResult PublishReleaseMessageToTopicFlag()
        {
            ProduceMessage(new Message<Null, string>() { Value = Configuration.ReleaseFlag }, Configuration.TopicFlag, Configuration.PartitionId); ;
            return Ok();
        }

        [HttpPost]
        [Route("PublishMessageToTopicCash{amountOfMessage}")]
        public IActionResult PublishMessageToTopicCash(int amountOfMessage)
        {
            for (int i = 0; i < amountOfMessage; i++)
            {
                ProduceMessage(new Message<Null, string>() { Value = "message " + i }, Configuration.TopicCash, Configuration.PartitionId); ;
            }

            return Ok();
        }

        private void ProduceMessage(Message<Null, string> message, string topic, int partiton)
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
