use crate::aether_msg::{msg,msg_manage};
use crate::aether_msg::msg::UserInfo;

pub struct Session {
    id: String,
    participant: Vec<UserInfo>
    
}
