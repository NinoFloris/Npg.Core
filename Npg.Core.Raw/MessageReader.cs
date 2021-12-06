using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Npg.Core.Raw;

public enum BackendMessageCode : byte
{
    AuthenticationRequest   = (byte)'R',
    BackendKeyData          = (byte)'K',
    BindComplete            = (byte)'2',
    CloseComplete           = (byte)'3',
    CommandComplete         = (byte)'C',
    CopyData                = (byte)'d',
    CopyDone                = (byte)'c',
    CopyBothResponse        = (byte)'W',
    CopyInResponse          = (byte)'G',
    CopyOutResponse         = (byte)'H',
    DataRow                 = (byte)'D',
    EmptyQueryResponse      = (byte)'I',
    ErrorResponse           = (byte)'E',
    FunctionCall            = (byte)'F',
    FunctionCallResponse    = (byte)'V',
    NoData                  = (byte)'n',
    NoticeResponse          = (byte)'N',
    NotificationResponse    = (byte)'A',
    ParameterDescription    = (byte)'t',
    ParameterStatus         = (byte)'S',
    ParseComplete           = (byte)'1',
    PasswordPacket          = (byte)' ',
    PortalSuspended         = (byte)'s',
    ReadyForQuery           = (byte)'Z',
    RowDescription          = (byte)'T',
}

public struct Message
{
    public Message(BackendMessageCode code, int messageLength)
    {
        Code = code;
        MessageLength = messageLength;
    }
    
    public BackendMessageCode Code { get; }
    public int MessageLength { get; }
}
    
public ref struct MessageReader
{
    private const int CodeAndLengthBytes = 5;
    private readonly ReadOnlySequence<byte> _buffer;
    internal SequenceReader<byte> _reader;

    public MessageReader(ReadOnlySequence<byte> buffer)
    {
        _buffer = buffer;
        _reader = new SequenceReader<byte>(_buffer);
        Current = default;
    }
    
    public Message Current { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; private set; }

    public ReadOnlySpan<byte> UnreadSpan => _reader.UnreadSpan;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        if (!TryParseMessageHeader(out var code, out var length) || Remaining < length) 
            return false;
    
        _reader.Advance(length);
        Current = new Message(code, length);    
        return true;
    }
    
    public bool TryParseMessageHeader(out BackendMessageCode code, out int messageLength)
    {
        if (Remaining < CodeAndLengthBytes)
        {
            code = default;
            messageLength = default;
            return false;
        }

        byte cd;
        int i;
        var span = UnreadSpan;
        if (span.Length >= CodeAndLengthBytes)
        {
            cd = span[0];
            i = Unsafe.ReadUnaligned<int>(ref MemoryMarshal.GetReference(span.Slice(1)));
            if (BitConverter.IsLittleEndian)
                i = BinaryPrimitives.ReverseEndianness(i);
        }
        else
        {
            // Rewinds can be slow... and there are no peek or copy block methods yet (multiple open issues on github).
            _reader.TryRead(out cd);
            _reader.TryReadBigEndian(out i);
            _reader.Rewind(CodeAndLengthBytes);
        }
    
        code = (BackendMessageCode)cd;
        messageLength = i + 1;
        Debug.Assert(Enum.IsDefined(code));
        Debug.Assert(messageLength < 8192);
        return true;
    }
    
    public long Remaining => _reader.Remaining;
    public long Consumed => _reader.Consumed;
    public long Length => _reader.Length;
}

// These work around the fact we cannot return 'this' or instance fields from a struct method directly.
public static class MessageReaderExtensions
{
    // Does not work, see https://github.com/dotnet/roslyn/issues/58122
    // public static MessageReader GetEnumerator(this ref MessageReader reader) => reader;

    public static ref SequenceReader<byte> GetReader(this ref MessageReader reader) => ref reader._reader;
}