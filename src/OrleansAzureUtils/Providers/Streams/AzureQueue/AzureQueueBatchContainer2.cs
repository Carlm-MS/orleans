using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Orleans.CodeGeneration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Orleans.Providers.Streams.AzureQueue
{
    [RegisterSerializer]
    internal class AzureQueueBatchContainer2 : AzureQueueBatchContainer
    {
        private static readonly int MaxChunkSize = (int)CloudQueueMessage.MaxMessageSize - (16 + sizeof(byte) + sizeof(int) + sizeof(int) + sizeof(int));

        [JsonConstructor]
        internal AzureQueueBatchContainer2(
            Guid streamGuid, 
            string streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext, 
            EventSequenceToken sequenceToken)
            : base(streamGuid, streamNamespace, events, requestContext, sequenceToken)
        {
        }

        internal AzureQueueBatchContainer2(
            Guid streamGuid, 
            string streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext)
            : base(streamGuid, streamNamespace, events, requestContext)
        {
        }

        private AzureQueueBatchContainer2() : base()
        {
        }

        /// <summary>
        /// Creates a deep copy of an object
        /// </summary>
        /// <param name="original">The object to create a copy of</param>
        /// <returns>The copy.</returns>
        public static object DeepCopy(object original)
        {
            var source = original as AzureQueueBatchContainer2;
            if (source == null)
            {
                throw new ArgumentNullException(nameof(original));
            }

            var copy = new AzureQueueBatchContainer2();
            SerializationContext.Current.RecordObject(original, copy);
            var token = source.sequenceToken == null ? null : new EventSequenceToken2(source.sequenceToken.SequenceNumber, source.sequenceToken.EventIndex);
            var events = source.events?.Select(SerializationManager.DeepCopyInner).ToList();
            var context = source.requestContext?.ToDictionary(kv => kv.Key, kv => SerializationManager.DeepCopyInner(kv.Value));
            copy.SetValues(source.StreamGuid, source.StreamNamespace, events, context, token);
            return copy;
        }

        /// <summary>
        /// Serializes the container to the binary stream.
        /// </summary>
        /// <param name="untypedInput">The object to serialize</param>
        /// <param name="writer">The stream to write to</param>
        /// <param name="expected">The expected type</param>
        public static void Serialize(object untypedInput, BinaryTokenStreamWriter writer, Type expected)
        {
            var typed = untypedInput as AzureQueueBatchContainer2;
            if (typed == null)
            {
                throw new SerializationException();
            }

            writer.Write(typed.StreamGuid);
            writer.Write(typed.StreamNamespace);
            SerializationManager.SerializeInner(typed.sequenceToken, writer, typed.sequenceToken?.GetType());
            SerializationManager.SerializeInner(typed.events, writer, typed.events?.GetType());
            SerializationManager.SerializeInner(typed.requestContext, writer, typed.requestContext?.GetType());
        }

        /// <summary>
        /// Deserializes the container from the data stream.
        /// </summary>
        /// <param name="expected">The expected type</param>
        /// <param name="reader">The stream reader</param>
        /// <returns>The deserialized value</returns>
        public static object Deserialize(Type expected, BinaryTokenStreamReader reader)
        {
            var deserialized = new AzureQueueBatchContainer2();
            DeserializationContext.Current.RecordObject(deserialized);

            var guid = reader.ReadGuid();
            var ns = reader.ReadString();
            var eventToken = Deserialize<EventSequenceToken2>(reader);
            var events = Deserialize<List<object>>(reader);
            var context = Deserialize<Dictionary<string, object>>(reader);
            deserialized.SetValues(guid, ns, events, context, eventToken);
            return deserialized;
        }

        /// <summary>
        /// Register the serializer methods.
        /// </summary>
        public static void Register()
        {
            SerializationManager.Register(typeof(AzureQueueBatchContainer2), DeepCopy, Serialize, Deserialize);
        }

        internal static IEnumerable<CloudQueueMessage> ToCloudQueueMessages<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var azureQueueBatchMessage = new AzureQueueBatchContainer2(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = SerializationManager.SerializeToByteArray(azureQueueBatchMessage);
            var segments = rawBytes.ToSegments(MaxChunkSize).ToList();
            var guid = Guid.NewGuid();
            for (var i = 0; i < segments.Count; i++)
            {
                var chunk = new MessageChunk
                {
                    Guid = guid,
                    TotalChunks = segments.Count,
                    ChunkIndex = i,
                    Payload = segments[i]
                };

                yield return new CloudQueueMessage(chunk.ToByteArray());
            }
        }

        internal static IEnumerable<AzureQueueBatchContainer2> FromMessageChunks(IList<MessageChunk> chunks, ref long lastReadMessage)
        {
            var dictionary = chunks.GroupBy(c => c.Guid).ToDictionary(c => c.Key, c => c.ToList());
            var completeLists = dictionary.Values.Where(v => v.Count == v[0].TotalChunks);
            foreach (var item in completeLists.SelectMany(item => item))
            {
                chunks.Remove(item);
            }

            var result = new List<AzureQueueBatchContainer2>();
            foreach (var list in completeLists)
            {
                var bytes = MessageChunk.Join(list.OrderBy(li => li.ChunkIndex));
                var container = SerializationManager.DeserializeFromByteArray<AzureQueueBatchContainer2>(bytes);
                container.sequenceToken = new EventSequenceToken(lastReadMessage++);
                result.Add(container);
            }

            return result;
        }

        private void SetValues(Guid streamGuid, string streamNamespace, List<object> events, Dictionary<string, object> requestContext, EventSequenceToken2 sequenceToken)
        {
            this.StreamGuid = streamGuid;
            this.StreamNamespace = streamNamespace;
            this.events = events;
            this.requestContext = requestContext;
            this.sequenceToken = sequenceToken;
        }

        private static T Deserialize<T>(BinaryTokenStreamReader reader) where T: class
        {
            if (reader.PeekToken() == SerializationTokenType.Null)
            {
                reader.ReadToken();
                return null;
            }
            else
            {
                return SerializationManager.DeserializeInner<T>(reader);
            }                
        } 
    }
}
