using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.WindowsAzure.Storage.Queue;
using Orleans.Providers.Streams.AzureQueue;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.ServiceBus.Providers;
using OrleansServiceBus.Providers.Streams.EventHub;
using Xunit;

namespace UnitTests.Serialization
{
    public class StreamTypeSerializationTests
    {
        public StreamTypeSerializationTests()
        {
            // FakeSerializer definied in ExternalSerializerTest.cs
            SerializationManager.InitializeForTesting(new List<TypeInfo> { typeof(FakeSerializer).GetTypeInfo() });
            EventSequenceToken2.Register();
            EventHubSequenceToken2.Register();
            AzureQueueBatchContainer2.Register();
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventSequenceToken_VerifyStillUsingFallbackSerializer()
        {
            var token = new EventSequenceToken(long.MaxValue, int.MaxValue);
            VerifyUsingFallbackSerializer(token);
   
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer_VerifyStillUsingFallbackSerializer()
        {
            var container = new AzureQueueBatchContainer(Guid.NewGuid(), "namespace", new List<object> {"item"}, new Dictionary<string, object>() {{"key", "value"}}, new EventSequenceToken(long.MaxValue, int.MaxValue));
            VerifyUsingFallbackSerializer(container);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer_VerifyThatDeserializationIsBackwardCompatible()
        {
            var guid = Guid.NewGuid();
            var message = AzureQueueBatchContainer.ToCloudQueueMessage(guid, "namespace", new List<object> { "item" }, new Dictionary<string, object>() { { "key", "value" } });
            MessageChunk chunk;
            Assert.False(MessageChunk.TryCreateFromCloudQueueMessage(message, out chunk));
            var container = AzureQueueBatchContainer.FromCloudQueueMessage(message, 0);
            Assert.Equal(guid, container.StreamGuid);
            Assert.Equal("namespace", container.StreamNamespace);
            var events = container.GetEvents<string>();
            Assert.NotNull(events);
            Assert.Equal(1, events.Count());
            Assert.Equal("item", events.First().Item1);
            Assert.Equal(1, container.RequestContext.Count);
            Assert.Equal("key", container.RequestContext.Keys.First());
            Assert.Equal("value", container.RequestContext.Values.First());
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer_VerifyCanDeserializeAzureQueueBatchContainer2()
        {
            var container = new AzureQueueBatchContainer2(Guid.NewGuid(), "namespace", new List<object> {"item"}, new Dictionary<string, object>() {{"key", "value"}}, new EventSequenceToken2(long.MaxValue, int.MaxValue));
            var bytes = SerializationManager.SerializeToByteArray(container);
            var message = new CloudQueueMessage(bytes);
            SerializationManager.DeserializeFromByteArray<AzureQueueBatchContainer>(message.AsBytes);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventHubSequenceToken_VerifyStillUsingFallbackSerializer()
        {
            var token = new EventHubSequenceToken("some offset", long.MaxValue, int.MaxValue);
            VerifyUsingFallbackSerializer(token);
        }

        private static void VerifyUsingFallbackSerializer(object ob)
        {
            var writer = new BinaryTokenStreamWriter();
            SerializationManager.FallbackSerializer(ob, writer, ob.GetType());
            var bytes = writer.ToByteArray();

            byte[] defaultFormatterBytes;
            var formatter = new BinaryFormatter();
            using (var stream = new MemoryStream())
            {
                formatter.Serialize(stream, ob);
                stream.Flush();
                defaultFormatterBytes = stream.ToArray();
            }

            var reader = new BinaryTokenStreamReader(bytes);
            var serToken = reader.ReadToken();
            Assert.Equal(SerializationTokenType.Fallback, serToken);
            var length = reader.ReadInt();
            Assert.Equal(length, defaultFormatterBytes.Length);
            var segment = new ArraySegment<byte>(bytes, reader.CurrentPosition, bytes.Length - reader.CurrentPosition);
            Assert.True(segment.SequenceEqual(defaultFormatterBytes));
        }

        #region EventSequenceToken2

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventSequenceToken2_DeepCopy_IfNotNull()
        {
            var token = new EventSequenceToken2(long.MaxValue, int.MaxValue);
            var copy = EventSequenceToken2.DeepCopy(token) as EventSequenceToken;
            Assert.NotNull(copy);
            Assert.NotSame(token, copy);
            Assert.Equal(token.EventIndex, copy.EventIndex);
            Assert.Equal(token.SequenceNumber, copy.SequenceNumber);

            var writer = new BinaryTokenStreamWriter();
            SerializationManager.Serialize(token, writer);
            var bytes = writer.ToByteArray();

            var reader = new BinaryTokenStreamReader(bytes);
            copy = SerializationManager.Deserialize(reader) as EventSequenceToken;
            Assert.NotNull(copy);
            Assert.NotSame(token, copy);
            Assert.Equal(token.EventIndex, copy.EventIndex);
            Assert.Equal(token.SequenceNumber, copy.SequenceNumber);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventSequenceToken2_Serialize_IfNotNull()
        {
            var writer = new BinaryTokenStreamWriter();
            var token = new EventSequenceToken2(long.MaxValue, int.MaxValue);
            EventSequenceToken2.Serialize(token, writer, null);
            var reader = new BinaryTokenStreamReader(writer.ToByteArray());
            var deserialized = EventSequenceToken2.Deserialize(typeof(EventSequenceToken2), reader) as EventSequenceToken2;
            Assert.NotNull(deserialized);
            Assert.NotSame(token, deserialized);
            Assert.Equal(token.EventIndex, deserialized.EventIndex);
            Assert.Equal(token.SequenceNumber, deserialized.SequenceNumber);
        }

        #endregion

        #region EventHubSequenceToken2

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventHubSequenceToken2_DeepCopy_IfNotNull()
        {
            var token = new EventHubSequenceToken2("name", long.MaxValue, int.MaxValue);
            var copy = EventHubSequenceToken2.DeepCopy(token) as EventSequenceToken;
            Assert.NotNull(copy);
            Assert.NotSame(token, copy);
            Assert.Equal(token.EventIndex, copy.EventIndex);
            Assert.Equal(token.SequenceNumber, copy.SequenceNumber);

            var writer = new BinaryTokenStreamWriter();
            SerializationManager.Serialize(token, writer);
            var bytes = writer.ToByteArray();

            var reader = new BinaryTokenStreamReader(bytes);
            copy = SerializationManager.Deserialize(reader) as EventHubSequenceToken2;
            Assert.NotNull(copy);
            Assert.NotSame(token, copy);
            Assert.Equal(token.EventIndex, copy.EventIndex);
            Assert.Equal(token.SequenceNumber, copy.SequenceNumber);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void EventHubSequenceToken2_Serialize_IfNotNull()
        {
            var writer = new BinaryTokenStreamWriter();
            var token = new EventHubSequenceToken2("name", long.MaxValue, int.MaxValue);
            EventHubSequenceToken2.Serialize(token, writer, null);
            var reader = new BinaryTokenStreamReader(writer.ToByteArray());
            var deserialized = EventHubSequenceToken2.Deserialize(typeof (EventHubSequenceToken2), reader) as EventHubSequenceToken2;
            Assert.NotNull(deserialized);
            Assert.NotSame(token, deserialized);
            Assert.Equal(token.EventIndex, deserialized.EventIndex);
            Assert.Equal(token.SequenceNumber, deserialized.SequenceNumber);
        }
        #endregion

        #region AzureQueueBatchContainer2

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer2_DeepCopy_IfNotNullAndUsingExternalSerializer()
        {
            var container = CreateAzureQueueBatchContainer();
            var copy = AzureQueueBatchContainer2.DeepCopy(container) as AzureQueueBatchContainer2;
            ValidateIdenticalQueueBatchContainerButNotSame(container, copy);
            copy = SerializationManager.DeepCopy(container) as AzureQueueBatchContainer2;
            ValidateIdenticalQueueBatchContainerButNotSame(container, copy);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer2_Serialize_IfNotNull()
        {
            var container = CreateAzureQueueBatchContainer();
            var writer = new BinaryTokenStreamWriter();
            AzureQueueBatchContainer2.Serialize(container, writer, null);
            var reader = new BinaryTokenStreamReader(writer.ToByteArray());
            var deserialized = AzureQueueBatchContainer2.Deserialize(typeof (AzureQueueBatchContainer), reader) as AzureQueueBatchContainer2;
            ValidateIdenticalQueueBatchContainerButNotSame(container, deserialized);

            writer = new BinaryTokenStreamWriter();
            SerializationManager.Serialize(container, writer);
            reader = new BinaryTokenStreamReader(writer.ToByteArray());
            deserialized = SerializationManager.Deserialize<AzureQueueBatchContainer2>(reader);
            ValidateIdenticalQueueBatchContainerButNotSame(container, deserialized);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Serialization")]
        public void AzureQueueBatchContainer2_Serialize_MessagesLargerThanMaxCloudQueueMessage()
        {
            var random = new Random();
            var events = Enumerable.Range(0, 1000).Select(i =>
                {
                    var arr = new byte[1000];
                    random.NextBytes(arr);
                    return arr;
                }).ToList();
            var events2 = Enumerable.Range(0, 1000).Select(i =>
            {
                var arr = new byte[1000];
                random.NextBytes(arr);
                return arr;
            }).ToList();

            var messages = AzureQueueBatchContainer2.ToCloudQueueMessages(Guid.NewGuid(), "some namespace", events, null);
            var messages2 = AzureQueueBatchContainer2.ToCloudQueueMessages(Guid.NewGuid(), "some other namespace", events2, null);
            var allMessages = messages.Concat(messages2);
            var chunks = new List<MessageChunk>();
            foreach (var message in allMessages)
            {
                MessageChunk chunk;
                if (MessageChunk.TryCreateFromCloudQueueMessage(message, out chunk))
                {
                    chunks.Add(chunk);
                }
            }

            var sequence = 0L;
            var containers = AzureQueueBatchContainer2.FromMessageChunks(chunks, ref sequence).ToList();
            Assert.Equal(2, containers.Count);
            var newEvents = containers[0].GetEvents<byte[]>().Select(t => t.Item1).ToList();
            var newEvents2 = containers[1].GetEvents<byte[]>().Select(t => t.Item1).ToList();
            Assert.NotNull(newEvents);
            Assert.NotNull(newEvents2);
            Assert.Equal(1000, newEvents.Count());
            Assert.Equal(1000, newEvents2.Count());
            for (var i = 0; i < 1000; i++)
            {
                Assert.True(events[i].SequenceEqual(newEvents[i]));
                Assert.True(events2[i].SequenceEqual(newEvents2[i]));
            }
        }



        private static AzureQueueBatchContainer2 CreateAzureQueueBatchContainer()
        {
            return new AzureQueueBatchContainer2(
                Guid.NewGuid(), 
                "some namespace", 
                new List<object> { new FakeSerialized { SomeData = "text"} }, 
                new Dictionary<string, object>
                {
                    { "some key", new FakeSerialized { SomeData = "text 2" } }
                },
                new EventSequenceToken2(long.MaxValue, int.MaxValue));
        }

        private static void ValidateIdenticalQueueBatchContainerButNotSame(AzureQueueBatchContainer2 orig, AzureQueueBatchContainer2 copy)
        {
            Assert.NotNull(copy);
            Assert.NotSame(orig, copy);
            Assert.NotNull(copy.SequenceToken);
            Assert.Equal(orig.SequenceToken, copy.SequenceToken);
            Assert.NotSame(orig.SequenceToken, copy.SequenceToken);
            Assert.Equal(orig.StreamGuid, copy.StreamGuid);
            Assert.Equal(orig.StreamNamespace, copy.StreamNamespace);
            Assert.Equal(orig.StreamNamespace, "some namespace");
            Assert.NotNull(copy.RequestContext);
            Assert.NotSame(orig.RequestContext, copy.RequestContext);
            foreach (var kv in orig.RequestContext)
            {
                Assert.True(copy.RequestContext.ContainsKey(kv.Key));
            }

            Assert.True(copy.RequestContext.ContainsKey("some key"));
        }

        #endregion
    }
}
