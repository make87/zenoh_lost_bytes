import datetime
import hashlib
import time

import zenoh

from image_jpeg_pb2 import ImageJPEG


start = datetime.datetime.now()


def callback(sample: zenoh.Sample):
    message_bytes = sample.payload.to_bytes()

    sha256_hash = hashlib.sha256(message_bytes).hexdigest()
    original_hash = sample.attachment.to_string()

    assert (
        sha256_hash == original_hash
    ), f"Hash mismatch: {sha256_hash} != {original_hash}  after {datetime.datetime.now() - start} seconds"

    message = ImageJPEG()
    message.ParseFromString(message_bytes)


def main():
    session = zenoh.open(config=zenoh.Config())
    session.declare_subscriber("topic", callback)

    print("Start receiving silently...")

    time.sleep(1_000)  # keep application open so subscriber can receive messages


if __name__ == "__main__":
    main()
