using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Npg.Core.Raw
{
    public sealed class PipeReadBuffer
    {
        private long _readPosition;
        private ReadOnlySequence<byte> _buffer = ReadOnlySequence<byte>.Empty;
        private long _bufferLength;
        private readonly PipeReader _input;
        
        public PipeReadBuffer(PipeReader input) => _input = input;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRead(int minimumSize, out ReadOnlySequence<byte> buffer)
        {
            if (!_buffer.IsEmpty)
            {
                _input.AdvanceTo( _buffer.GetPosition(_readPosition),_buffer.GetPosition(Math.Max(_readPosition, _bufferLength - 1)));
                _readPosition = 0;
                _buffer = ReadOnlySequence<byte>.Empty;
                _bufferLength = 0;
            }
            
            if (_input.TryRead(out var result))
            {
                var buf = result.Buffer;
                var bufferLength = buf.Length;

                if (bufferLength >= minimumSize)
                {
                    buffer =_buffer = buf;
                    _bufferLength = bufferLength;
                    return true;
                }

                _input.AdvanceTo(buf.Start, buf.End);
            }
            
            buffer = ReadOnlySequence<byte>.Empty;
            return false;
        }
        
        public ValueTask WaitForDataAsync(int minimumSize, CancellationToken cancellationToken = default)
        {
            if (_readPosition == 0 && _bufferLength >= minimumSize || TryRead(minimumSize, out _))
                return ValueTask.CompletedTask;
            
            return WaitForDataAsyncCore(this, minimumSize, cancellationToken);
            
            static async ValueTask WaitForDataAsyncCore(PipeReadBuffer instance, int minimumSize, CancellationToken cancellationToken)
            {
                while (true)
                {
                    var result = await instance._input.ReadAsync(cancellationToken).ConfigureAwait(false);
                    if (result.IsCompleted || result.IsCanceled)
                        throw result.IsCompleted ? new ObjectDisposedException("Pipe was completed while waiting for more data.") : new OperationCanceledException();
                
                    var buffer = result.Buffer;
                    var bufferLength = buffer.Length;
                    if (bufferLength >= minimumSize)
                    {
                        // Immediately advance so TryRead can reread.
                        instance._input.AdvanceTo( buffer.Start,buffer.GetPosition(bufferLength - 1));
                        break;
                    }
                
                    // Keep buffering until we get more data
                    instance._input.AdvanceTo(buffer.Start, buffer.End);
                }
            }
        }

        public void Advance(long bytes) => _readPosition += bytes;
    }
}
