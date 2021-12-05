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

    public MessageReader(ReadOnlySequence<byte> buffer)
    {
        _buffer = buffer;
        _reader = default;
        HasReader = false;
        Current = default;
    }
    
    public Message Current { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; private set; }

    public ReadOnlySpan<byte> UnreadSpan
    {
        get
        {
            EnsureReader();
            return _reader.UnreadSpan;
        }
    }

    internal bool HasReader { get; private set; }
    // Hacky internal field to deal with the fact we can't ref return instance fields from properties...
    internal SequenceReader<byte> _reader;

    internal void EnsureReader()
    {
        if (HasReader) return;
        HasReader = true;
        _reader = new SequenceReader<byte>(_buffer);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        if (!HasReader) EnsureReader();
        if (!TryParseMessageHeader(out var code, out var length) || Remaining < length) 
            return false;
    
        _reader.Advance(length);
        Current = new Message(code, length);    
        return true;
    }
    
    public bool TryParseMessageHeader(out BackendMessageCode code, out int frameLength)
    {
        if (Remaining < CodeAndLengthBytes)
        {
            code = default;
            frameLength = default;
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
            EnsureReader();
            // Rewinds can be slow... and there are no peek or copy block methods yet (multiple open issues on github).
            _reader.TryRead(out cd);
            _reader.TryReadBigEndian(out i);
            _reader.Rewind(CodeAndLengthBytes);
        }
    
        code = (BackendMessageCode)cd;
        frameLength = i + 1;
        Debug.Assert(Enum.IsDefined(code));
        Debug.Assert(frameLength < 8192);
        return true;
    }
    
    public long Remaining => HasReader ? _reader.Remaining : _buffer.Length;
    public long Consumed => HasReader ? _reader.Consumed : 0;
    
    public ReadOnlySequence<byte> Sequence => _buffer;
}

// These work around the fact we cannot return 'this' or instance fields from a struct method directly.
public static class MessageReaderExtensions
{
    // Does not work, see https://github.com/dotnet/roslyn/issues/58122
    // public static MessageReader GetEnumerator(this ref MessageReader reader) => reader;

    public static ref SequenceReader<byte> GetReader(this ref MessageReader reader)
    {
        if (!reader.HasReader) reader.EnsureReader();
        return ref reader._reader;
    }
}