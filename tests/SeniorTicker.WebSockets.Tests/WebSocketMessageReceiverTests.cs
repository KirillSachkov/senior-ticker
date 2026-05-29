using System.Text;
using SeniorTicker.Infrastructure.WebSockets;
using Xunit;

namespace SeniorTicker.WebSockets.Tests;

public class WebSocketMessageReceiverTests
{
    [Fact]
    public async Task Reassembles_a_multi_fragment_message_larger_than_initial_buffer()
    {
        // 20 KB сообщение при начальном буфере 4 KB — буфер должен вырасти (фикс #8)
        var payload = Encoding.UTF8.GetBytes(new string('x', 20_000));
        var socket = new FakeWebSocket(
        [
            (payload[..8000], false, false),
            (payload[8000..16000], false, false),
            (payload[16000..], true, false),
        ]);
        var receiver = new WebSocketMessageReceiver(initialBufferBytes: 4096, maxMessageBytes: 256 * 1024);

        using var msg = await receiver.ReceiveAsync(socket, CancellationToken.None);

        Assert.False(msg.IsClosed);
        Assert.Equal(20_000, msg.Span.Length);
        Assert.True(msg.Span.SequenceEqual(payload));
    }

    [Fact]
    public async Task Throws_when_message_exceeds_max()
    {
        var payload = Encoding.UTF8.GetBytes(new string('y', 5000));
        var socket = new FakeWebSocket([(payload, false, false), (payload, false, false)]);
        var receiver = new WebSocketMessageReceiver(initialBufferBytes: 1024, maxMessageBytes: 2048);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => { using var _ = await receiver.ReceiveAsync(socket, CancellationToken.None); });
    }

    [Fact]
    public async Task Returns_closed_on_close_frame()
    {
        var socket = new FakeWebSocket([(Array.Empty<byte>(), true, true)]);
        var receiver = new WebSocketMessageReceiver(1024, 2048);
        using var msg = await receiver.ReceiveAsync(socket, CancellationToken.None);
        Assert.True(msg.IsClosed);
    }
}
