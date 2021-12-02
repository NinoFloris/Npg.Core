using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipeReadBuffer
    {
        private int _readPosition;
        private ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
        private readonly PipeReader _input;

        public PipeReadBuffer(PipeReader input)
        {
            this._input = input;
        }

        public bool TryEnsure(int bytes, out ReadOnlySequence<byte> buffer)
        {
            if (_readPosition + bytes <= _buffer.Length)
            {
                buffer = _buffer.Slice(_readPosition);
                return true;
            }
            
            Reset();
            
            if (_input.TryRead(out var result))
            {
                var buf = result.Buffer;
                if (buf.Length >= bytes)
                {
                    _buffer = buffer = buf;
                    return true;
                }

                _input.AdvanceTo(buf.Start, buf.End);
            }
            
            buffer = ReadOnlySequence<byte>.Empty;
            return false;
        }

        public ValueTask<ReadOnlySequence<byte>> EnsureAsync(int bytes)
        {
            if (TryEnsure(bytes, out var buffer))
                return new ValueTask<ReadOnlySequence<byte>>(buffer);

            return EnsureAsyncCore(this, bytes);
            
            static async ValueTask<ReadOnlySequence<byte>> EnsureAsyncCore(PipeReadBuffer instance, int bytes)
            {
                var r = await instance._input.ReadAtLeastAsync(bytes);
                return instance._buffer = r.Buffer;
            }
        }


        private void Reset()
        {
            if (!_buffer.IsEmpty)
            {
                _input.AdvanceTo(_buffer.GetPosition(_readPosition), _buffer.End);
                _readPosition = 0;
                _buffer = ReadOnlySequence<byte>.Empty;
            }
        }

        public void Consume(int bytes) => _readPosition += bytes;
    }
}
