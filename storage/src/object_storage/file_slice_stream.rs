/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::io::{self, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};
use tokio_util::io::ReaderStream;

/// Offers a stream of byte over a specific range of bytes in a file.
///
/// This struct is useful when uploading an object to S3.
#[derive(Debug)]
pub struct FileSliceStream<R> {
    inner: ReaderStream<R>,
    remaining: u64,
}

impl<R> FileSliceStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    pub async fn try_new(mut reader: R, range: Range<u64>) -> io::Result<Self> {
        assert!(range.end >= range.start);

        let seek_from = SeekFrom::Start(range.start);
        reader.seek(seek_from).await?;

        Ok(FileSliceStream {
            inner: ReaderStream::new(reader),
            remaining: range.end - range.start,
        })
    }
}

impl<R> Stream for FileSliceStream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        let mut polled = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(ref mut bytes))) = polled {
            bytes.truncate(self.remaining as usize); // no-op when bytes.len() < remaining
            self.remaining -= bytes.len() as u64;
        }

        polled
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::StreamExt;
    use std::io::Cursor;

    use crate::object_storage::file_slice_stream::FileSliceStream;

    /*
    #[tokio::test]
    async fn test_split_into_upload_file_part_requests() -> anyhow::Result<()> {
        let bytes = b"abcdef";

        let mut file = tempfile::NamedTempFile::new()?;
        file.write(&bytes[..])?;

        let upload_file_request = UploadFileRequest {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            path: file.path().to_path_buf(),
            content_length: Some(6),
        };

        let upload_file_part_requests =
            SplitWriter::split_into_upload_file_part_requests("upload_id", &upload_file_request, 3)
                .await?;

        assert_eq!(upload_file_part_requests.len(), 2);

        assert_eq!(upload_file_part_requests[0].bucket, "bucket".to_string());
        assert_eq!(upload_file_part_requests[0].key, "key".to_string());
        assert_eq!(upload_file_part_requests[0].path, file.path().to_path_buf());
        assert_eq!(upload_file_part_requests[0].start_offset, 0);
        assert_eq!(upload_file_part_requests[0].end_offset, 3);
        assert_eq!(upload_file_part_requests[0].part_length, 3);
        assert_eq!(
            upload_file_part_requests[0].part_md5,
            format!("{:x}", md5::compute(b"abc"))
        );
        assert_eq!(
            upload_file_part_requests[0].upload_id,
            "upload_id".to_string()
        );

        assert_eq!(upload_file_part_requests[1].start_offset, 3);
        assert_eq!(upload_file_part_requests[1].end_offset, 6);
        assert_eq!(upload_file_part_requests[1].part_length, 3);
        assert_eq!(
            upload_file_part_requests[1].part_md5,
            format!("{:x}", md5::compute(b"def"))
        );

        // This time, part size is not a multiple of file size.
        let upload_file_part_requests =
            SplitWriter::split_into_upload_file_part_requests("upload_id", &upload_file_request, 5)
                .await?;

        assert_eq!(upload_file_part_requests.len(), 2);

        assert_eq!(upload_file_part_requests[0].start_offset, 0);
        assert_eq!(upload_file_part_requests[0].end_offset, 5);
        assert_eq!(upload_file_part_requests[0].part_length, 5);
        assert_eq!(
            upload_file_part_requests[0].part_md5,
            format!("{:x}", md5::compute(b"abcde"))
        );
        assert_eq!(
            upload_file_part_requests[0].upload_id,
            "upload_id".to_string()
        );

        assert_eq!(upload_file_part_requests[1].start_offset, 5);
        assert_eq!(upload_file_part_requests[1].end_offset, 6);
        assert_eq!(upload_file_part_requests[1].part_length, 1);
        assert_eq!(
            upload_file_part_requests[1].part_md5,
            format!("{:x}", md5::compute(b"f"))
        );

        Ok(())
    }
    */

    #[tokio::test]
    async fn test_file_slice_stream() -> anyhow::Result<()> {
        let bytes = b"abcdef";

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 0..0).await?;
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 10..15).await?;
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 0..1).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[..1]));
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 5..6).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[5..]));
        assert!(stream.next().await.is_none());

        let cursor = Cursor::new(&bytes[..]);
        let mut stream = FileSliceStream::try_new(cursor, 2..4).await?;
        assert_eq!(stream.next().await.unwrap()?, Bytes::from(&bytes[2..4]));
        assert!(stream.next().await.is_none());

        Ok(())
    }
}
