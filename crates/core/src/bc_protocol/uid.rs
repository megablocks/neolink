use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};

fn uid_request(channel_id: u8, msg_num: u16) -> Bc {
    Bc {
        meta: BcMeta {
            msg_id: MSG_ID_UID,
            channel_id,
            msg_num,
            response_code: 0,
            stream_type: 0,
            class: 0x6414,
        },
        body: BcBody::ModernMsg(ModernMsg {
            extension: None,
            payload: None,
        }),
    }
}

impl BcCamera {
    /// Get the [Uid] XML for one logical camera channel.
    pub async fn get_uid_for_channel(&self, channel_id: u8) -> Result<Uid> {
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut sub_get = connection.subscribe(MSG_ID_UID, msg_num).await?;

        sub_get.send(uid_request(channel_id, msg_num)).await?;
        let msg = sub_get.recv().await?;
        if msg.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: msg.meta.msg_id,
                code: msg.meta.response_code,
            });
        }

        if let BcBody::ModernMsg(ModernMsg {
            payload:
                Some(BcPayloads::BcXml(BcXml {
                    uid: Some(uid_xml), ..
                })),
            ..
        }) = msg.body
        {
            Ok(uid_xml)
        } else {
            Err(Error::UnintelligibleReply {
                reply: std::sync::Arc::new(Box::new(msg)),
                why: "Expected Uid xml but it was not received",
            })
        }
    }

    /// Get the [Uid] XML for the camera's configured logical channel.
    pub async fn get_uid(&self) -> Result<Uid> {
        self.get_uid_for_channel(self.channel_id).await
    }

    /// Get the UID for one logical camera channel.
    pub async fn uid_for_channel(&self, channel_id: u8) -> Result<String> {
        Ok(self.get_uid_for_channel(channel_id).await?.uid)
    }

    /// Get the UID
    pub async fn uid(&self) -> Result<String> {
        self.uid_for_channel(self.channel_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uid_request_uses_explicit_logical_channel() {
        let request = uid_request(17, 42);
        assert_eq!(request.meta.channel_id, 17);
        assert_eq!(request.meta.msg_num, 42);
    }
}
