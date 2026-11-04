use super::*;
use crate::endpoints::language::{
    ENGLISH_SLOT, LanguagePack, LanguagePacks, LanguageResponse, MAX_LANGUAGE_PACKS,
    PACKS_RESPONSE, SELECT_RESPONSE, build_packs_request, build_select_request,
};

struct PendingLanguageRequest {
    inner: Arc<Mutex<PebbleInner>>,
    key: (u8, u8, Option<u8>),
}

impl Drop for PendingLanguageRequest {
    fn drop(&mut self) {
        self.inner
            .lock()
            .unwrap()
            .language_pending
            .remove(&self.key);
    }
}

impl Pebble {
    /// Query installed pack identities and the active slot. The caller must
    /// check SupportsMultipleLanguagePacks (watch capability bit 28) first.
    pub async fn list_language_packs(&self) -> Result<LanguagePacks, PebbleError> {
        let _operation = self.language_operation.lock().await;
        let token = rand_u16() as u8;
        match self
            .language_request(token, PACKS_RESPONSE, build_packs_request(token))
            .await?
        {
            LanguageResponse::Packs(packs) => Ok(packs),
            _ => Err(PebbleError::Other(
                "unexpected language pack response".into(),
            )),
        }
    }

    /// Select an installed translation, or built-in English (`None`). The
    /// watch checks the slot's version and locale so a replaced pack cannot
    /// accidentally be selected using an old snapshot.
    pub async fn set_active_language(
        &self,
        pack: Option<&LanguagePack>,
    ) -> Result<(), PebbleError> {
        if pack.is_some_and(|pack| {
            usize::from(pack.slot) >= MAX_LANGUAGE_PACKS
                || !pack.readable
                || !pack.has_translation
                || !pack.locale.is_ascii()
                || pack.locale.len() > 6
                || pack.locale.as_bytes().contains(&0)
        }) {
            return Err(PebbleError::Other("language pack is not selectable".into()));
        }
        let _operation = self.language_operation.lock().await;
        let token = rand_u16() as u8;
        let expected_slot = pack.map_or(ENGLISH_SLOT, |pack| pack.slot);
        match self
            .language_request(token, SELECT_RESPONSE, build_select_request(token, pack))
            .await?
        {
            LanguageResponse::Selected {
                slot,
                success: true,
            } if slot == expected_slot => Ok(()),
            _ => Err(PebbleError::Other(
                "watch rejected language selection".into(),
            )),
        }
    }

    async fn language_request(
        &self,
        token: u8,
        kind: u8,
        payload: Vec<u8>,
    ) -> Result<LanguageResponse, PebbleError> {
        let (sender, receiver) = oneshot::channel();
        let key = (kind, token, (kind == SELECT_RESPONSE).then(|| payload[3]));
        self.inner
            .lock()
            .unwrap()
            .language_pending
            .insert(key, sender);
        let _pending = PendingLanguageRequest {
            inner: Arc::clone(&self.inner),
            key,
        };
        self.send_pebble(Endpoint::SystemMessage, &payload)?;
        match tokio::time::timeout(Duration::from_secs(5), receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(PebbleError::NotConnected),
            Err(_) => Err(PebbleError::Other("language request timed out".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_removes_language_waiters() {
        let inner = Arc::new(Mutex::new(PebbleInner::new()));
        let (sender, _receiver) = oneshot::channel();
        let key = (PACKS_RESPONSE, 7, None);
        inner.lock().unwrap().language_pending.insert(key, sender);
        drop(PendingLanguageRequest {
            inner: inner.clone(),
            key,
        });
        assert!(inner.lock().unwrap().language_pending.is_empty());
    }

    #[tokio::test]
    async fn disconnect_closes_language_waiters() {
        let pebble = Pebble::new("00:00:00:00:00:00", "hci0");
        let (sender, receiver) = oneshot::channel();
        pebble
            .inner
            .lock()
            .unwrap()
            .language_pending
            .insert((PACKS_RESPONSE, 7, None), sender);
        pebble.disconnect().await;
        assert!(receiver.await.is_err());
    }
}
