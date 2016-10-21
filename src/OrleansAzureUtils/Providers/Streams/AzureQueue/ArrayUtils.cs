using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Streams.AzureQueue
{
    internal static class ArrayUtils
    {
        internal static IEnumerable<ArraySegment<T>> ToSegments<T>(this T[] arr, int segmentSize)
        {
            if (arr == null)
            {
                yield break;
            }

            for (var i = 0; i < arr.Length; i += segmentSize)
            {
                yield return new ArraySegment<T>(arr, i, i + segmentSize > arr.Length ? arr.Length - i : segmentSize);
            }
        }

        internal static int CopyTo<T>(this ArraySegment<T> segment, T[] arr, int offset)
        {
            Array.Copy(segment.Array, segment.Offset, arr, offset, segment.Count);
            return segment.Count;
        }

        internal static T[] Join<T>(this IEnumerable<ArraySegment<T>> segments)
        {
            var list = segments.ToList();
            var retVal = new T[list.Sum(s => s.Count)];
            var offset = 0;
            foreach (var segment in list)
            {
                offset += segment.CopyTo(retVal, offset);
            }

            return retVal;
        }

        internal static T[] Join<T>(this IEnumerable<T[]> segments)
        {
            var list = segments.ToList();
            var retVal = new T[list.Sum(s => s.Length)];
            var offset = 0;
            foreach (var segment in list)
            {
                Array.Copy(segment, 0, retVal, offset, segment.Length);
                offset += segment.Length;
            }

            return retVal;
        }

    }
}
