using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.WindowsAzure.Storage.Queue;
using Orleans.Serialization;

namespace Orleans.Providers.Streams.AzureQueue
{
    internal class MessageChunk
    {
        internal Guid Guid { get; set; }

        internal int TotalChunks { get; set; }
            
        internal int ChunkIndex { get; set; }

        internal ArraySegment<byte> Payload { get; set; }

        internal static MessageChunk FromBytes(byte[] bytes)
        {
            var reader = new BinaryTokenStreamReader(bytes);
            var b = reader.ReadByte();
            if (b != 255)
            {
                throw new SerializationException("Could deserialize message chunk");
            }

            var guid = reader.ReadGuid();
            var totalChunks = reader.ReadInt();
            var chunkIndex = reader.ReadInt();
            var arrayLength = reader.ReadInt();
            var payload = reader.ReadBytes(arrayLength);

            return new MessageChunk
            {
                Guid = guid,
                TotalChunks = totalChunks,
                ChunkIndex = chunkIndex,
                Payload = new ArraySegment<byte>(payload)
            };
        }

        internal static byte[] Join(IEnumerable<MessageChunk> chunks)
        {
            return chunks.OrderBy(c => c.ChunkIndex).Select(c => c.Payload).Join();
        }

        internal byte[] ToByteArray()
        {
            var writer = new BinaryTokenStreamWriter();

            // 0xFF is used to identify whether the serialized block is an AzureQueueBatchContainer or an
            // AzureBatchQueueContainer2. In the former case, the object is serialized by the fallback 
            // serializer which uses the .net binary formatter or the json serializer - neither of them
            // start with the byte 0xFF.
            writer.Write((byte)0xff);
            writer.Write(this.Guid);
            writer.Write(this.TotalChunks);
            writer.Write(this.ChunkIndex);
            writer.Write(this.Payload);
            return writer.ToByteArray();
        }

        internal static bool TryCreateFromCloudQueueMessage(CloudQueueMessage message, out MessageChunk chunk)
        {
            var bytes = message.AsBytes;
            if (bytes?.Length == 0 || bytes[0] != 255)
            {
                chunk = null;
                return false;
            }

            try
            {
                chunk = FromBytes(bytes);
                return true;
            }
            catch
            {
                chunk = null;
                return false;
            }
        }
    }
}
