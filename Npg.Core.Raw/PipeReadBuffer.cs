using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipeReadBuffer
    {
        private long _readPosition;
        private ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
        private readonly PipeReader _input;
        
        public PipeReadBuffer(PipeReader input) => _input = input;

        public bool TryRead(int minimumSize, out ReadOnlySequence<byte> buffer)
        {
            if (_readPosition + minimumSize <= _buffer.Length)
            {
                buffer = _buffer.Slice(_readPosition);
                return true;
            }
        
            return TryReadImpl(minimumSize, out buffer);
        }

        private bool TryReadImpl(int minimumSize, out ReadOnlySequence<byte> buffer)
        {
            if (!_buffer.IsEmpty)
            {
                _input.AdvanceTo(_buffer.GetPosition(_readPosition), _buffer.End);
                _readPosition = 0;
                _buffer = ReadOnlySequence<byte>.Empty;
            }
            
            if (_input.TryRead(out var result))
            {
                var buf = result.Buffer;
                if (buf.Length >= minimumSize)
                {
                    buffer =_buffer = buf;
                    return true;
                }

                _input.AdvanceTo(buf.Start, buf.End);
            }
            
            buffer = ReadOnlySequence<byte>.Empty;
            return false;
        }
        
        public async ValueTask EnsureAsync(int bytes, CancellationToken cancellationToken = default)
        {
            if (!TryReadImpl(bytes, out _))
            {
                var r = await _input.ReadAtLeastAsync(bytes, cancellationToken);
                _buffer = r.Buffer;
            }
        }

        public void Advance(long bytes) => _readPosition += bytes;
    }
}
