use std::future::Future;

use super::error::Error;
use crate::proto::protocol::{Attrs, Data, ExtendedReply, Handle, Name, Status, Version};

/// Client stream handler (native async trait).
pub trait Handler: Sized {
    type Error: Into<Error>;

    #[allow(unused_variables)]
    fn version(
        &mut self,
        version: Version,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn status(&mut self, status: Status) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn handle(&mut self, handle: Handle) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn data(&mut self, data: Data) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn name(&mut self, name: Name) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn attrs(&mut self, attrs: Attrs) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)]
    fn extended_reply(
        &mut self,
        reply: ExtendedReply,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}
