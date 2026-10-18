import contextlib
import json
import typing

import anyio
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.routing import Route

import httpx2

Message = typing.MutableMapping[str, typing.Any]
Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]]
Scope = typing.MutableMapping[str, typing.Any]


async def hello_world(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_path(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"path": scope["path"]}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_raw_path(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"raw_path": scope["raw_path"].decode("ascii")}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def echo_body(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    headers = [(b"content-type", "text/plain")]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    more_body = True
    while more_body:
        message = await receive()
        body = message.get("body", b"")
        more_body = message.get("more_body", False)
        await send({"type": "http.response.body", "body": body, "more_body": more_body})


async def echo_headers(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = json.dumps({"headers": [[k.decode(), v.decode()] for k, v in scope["headers"]]}).encode("utf-8")
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})


async def raise_exc(scope: Scope, receive: Receive, send: Send) -> None:
    raise RuntimeError()


async def raise_exc_after_response(scope: Scope, receive: Receive, send: Send) -> None:
    status = 200
    output = b"Hello, World!"
    headers = [(b"content-type", "text/plain"), (b"content-length", str(len(output)))]

    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": output})
    raise RuntimeError()


@pytest.mark.anyio
async def test_asgi_transport() -> None:
    async with httpx2.ASGITransport(app=hello_world) as transport:
        request = httpx2.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b"Hello, World!"


@pytest.mark.anyio
async def test_asgi_transport_no_body() -> None:
    async with httpx2.ASGITransport(app=echo_body) as transport:
        request = httpx2.Request("GET", "http://www.example.com/")
        response = await transport.handle_async_request(request)
        await response.aread()
        assert response.status_code == 200
        assert response.content == b""


@pytest.mark.anyio
async def test_asgi() -> None:
    transport = httpx2.ASGITransport(app=hello_world)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.text == "Hello, World!"


@pytest.mark.anyio
async def test_asgi_urlencoded_path() -> None:
    transport = httpx2.ASGITransport(app=echo_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path() -> None:
    transport = httpx2.ASGITransport(app=echo_raw_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/").copy_with(path="/user@example.org")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/user@example.org"}


@pytest.mark.anyio
async def test_asgi_raw_path_should_not_include_querystring_portion() -> None:
    """
    See https://github.com/encode/httpx/issues/2810
    """
    transport = httpx2.ASGITransport(app=echo_raw_path)
    async with httpx2.AsyncClient(transport=transport) as client:
        url = httpx2.URL("http://www.example.org/path?query")
        response = await client.get(url)

    assert response.status_code == 200
    assert response.json() == {"raw_path": "/path"}


@pytest.mark.anyio
async def test_asgi_upload() -> None:
    transport = httpx2.ASGITransport(app=echo_body)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert response.text == "example"


@pytest.mark.anyio
async def test_asgi_headers() -> None:
    transport = httpx2.ASGITransport(app=echo_headers)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.json() == {
        "headers": [
            ["host", "www.example.org"],
            ["accept", "*/*"],
            ["accept-encoding", "gzip, deflate, br, zstd"],
            ["connection", "keep-alive"],
            ["user-agent", f"python-httpx2/{httpx2.__version__}"],
        ]
    }


@pytest.mark.anyio
async def test_asgi_exc() -> None:
    transport = httpx2.ASGITransport(app=raise_exc)
    async with httpx2.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_exc_after_response() -> None:
    transport = httpx2.ASGITransport(app=raise_exc_after_response)
    async with httpx2.AsyncClient(transport=transport) as client:
        with pytest.raises(RuntimeError):
            await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_disconnect_after_response_complete() -> None:
    disconnect = False

    async def read_body(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal disconnect

        status = 200
        headers = [(b"content-type", "text/plain")]

        await send({"type": "http.response.start", "status": status, "headers": headers})
        more_body = True
        while more_body:
            message = await receive()
            more_body = message.get("more_body", False)

        await send({"type": "http.response.body", "body": b"", "more_body": False})

        # The ASGI spec says of the Disconnect message:
        # "Sent to the application when a HTTP connection is closed or if receive is
        # called after a response has been sent."
        # So if receive() is called again, the disconnect message should be received
        message = await receive()
        disconnect = message.get("type") == "http.disconnect"

    transport = httpx2.ASGITransport(app=read_body)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.post("http://www.example.org/", content=b"example")

    assert response.status_code == 200
    assert disconnect


@pytest.mark.anyio
async def test_asgi_exc_no_raise() -> None:
    transport = httpx2.ASGITransport(app=raise_exc, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

        assert response.status_code == 500


@pytest.mark.anyio
async def test_asgi_exc_without_context_manager() -> None:
    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=raise_exc))
    with pytest.raises(RuntimeError):
        await client.get("http://www.example.org/")


@pytest.mark.anyio
async def test_asgi_without_context_manager_buffers_response() -> None:
    disconnected = False

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal disconnected
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"Hello, ", "more_body": True})
        await send({"type": "http.response.body", "body": b"World!"})
        while (await receive())["type"] == "http.request":
            pass
        disconnected = True

    client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app))
    response = await client.get("http://www.example.org/")

    assert disconnected
    assert response.text == "Hello, World!"


@pytest.mark.anyio
async def test_asgi_exc_after_response_start() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        async with client.stream("GET", "http://www.example.org/") as response:
            assert response.status_code == 200
            with pytest.raises(RuntimeError):
                await response.aread()


@pytest.mark.anyio
async def test_asgi_exc_no_raise_after_response_start() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise RuntimeError()

    transport = httpx2.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx2.AsyncClient(transport=transport) as client:
        response = await client.get("http://www.example.org/")

    assert response.status_code == 200
    assert response.text == "partial"


@pytest.mark.anyio
async def test_asgi_streaming_response() -> None:
    chunks = iter([b"first", b"second"])
    next_chunk = anyio.Event()

    async def stream() -> typing.AsyncIterator[bytes]:
        yield next(chunks)
        await next_chunk.wait()
        yield next(chunks)

    async def endpoint(request: Request) -> StreamingResponse:
        return StreamingResponse(stream())

    app = Starlette(routes=[Route("/", endpoint)])

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                parts = response.aiter_bytes()
                assert await anext(parts) == b"first"
                next_chunk.set()
                assert await anext(parts) == b"second"
                with pytest.raises(StopAsyncIteration):
                    await anext(parts)


@pytest.mark.anyio
async def test_asgi_streaming_response_close_disconnects_app() -> None:
    disconnected = anyio.Event()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        async with anyio.create_task_group() as task_group:

            async def send_forever() -> None:
                while True:
                    await send({"type": "http.response.body", "body": b"chunk", "more_body": True})

            task_group.start_soon(send_forever)
            while (await receive())["type"] == "http.request":
                pass
            disconnected.set()
            task_group.cancel_scope.cancel()

    async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app)) as client:
        with anyio.fail_after(1):
            async with client.stream("GET", "http://www.example.org/") as response:
                async with contextlib.aclosing(response.aiter_bytes()) as parts:
                    assert await anext(parts) == b"chunk"
            await disconnected.wait()


@pytest.mark.anyio
async def test_asgi_unclosed_infinite_stream_is_cancelled_on_exit() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await anyio.sleep_forever()

    with anyio.fail_after(1):
        async with httpx2.ASGITransport(app=app) as transport:
            response = await transport.handle_async_request(httpx2.Request("GET", "http://www.example.org/"))
            assert response.status_code == 200
            await response.aclose()
