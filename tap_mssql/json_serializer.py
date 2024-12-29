from __future__ import annotations

import decimal
import typing as t

import msgspec


def _default_encoding(obj: t.Any) -> str:
    """Encoding helper for non native types.

    Args:
        obj: The object to encode.

    Returns:
        The encoded object.
    """
    return str(obj)


def _default_decoding(type_: type, obj: t.Any) -> t.Any:  # noqa: ARG001, ANN401
    """Decoding type helper for non native types.

    Args:
        type_: the type given
        obj: the item to be decoded
    Returns:
        The object converted to the appropriate type, default is str.
    """
    return str(obj)


encoder = msgspec.json.Encoder(enc_hook=_default_encoding, decimal_format="number")
decoder = msgspec.json.Decoder(dec_hook=_default_decoding, float_hook=decimal.Decimal)


def deserialize_json(json_str: str | bytes, **kwargs: t.Any) -> dict:
    """Deserialize a JSON string into a dictionary.

    Args:
        json_str: The JSON string to deserialize.
        **kwargs: Additional keyword arguments to pass to the decoder.

    Returns:
        The deserialized dictionary.
    """
    return decoder.decode(json_str)


def serialize_json(obj: object, **kwargs: t.Any) -> str:
    """Serialize an object into a JSON string.

    Args:
        obj: The object to serialize.
        **kwargs: Additional keyword arguments to pass to the encoder.

    Returns:
        The serialized JSON string.
    """
    return encoder.encode(obj).decode("utf-8")


msg_buffer = bytearray(64)


def serialize_jsonl(obj: object, **kwargs: t.Any) -> bytes:
    """Serialize an object into a JSONL string.

    Args:
        obj: The object to serialize.
        **kwargs: Additional keyword arguments to pass to the encoder.

    Returns:
        Bytes of the serialized JSON.
    """
    encoder.encode_into(obj, msg_buffer)
    msg_buffer.extend(b"\n")
    return msg_buffer
