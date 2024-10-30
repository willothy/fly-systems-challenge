use std::{future::Future, task::Poll};

pub trait DashMapAsync<'a, K, V>
where
    K: std::hash::Hash + Eq + Clone + 'a,
    V: 'a,
{
    fn entry_async(
        &'a self,
        key: K,
    ) -> impl Future<Output = dashmap::mapref::entry::Entry<'a, K, V>>;

    fn get_async(
        &'a self,
        key: &K,
    ) -> impl Future<Output = Option<dashmap::mapref::one::Ref<'a, K, V>>>;

    fn get_mut_async(
        &'a self,
        key: &K,
    ) -> impl Future<Output = Option<dashmap::mapref::one::RefMut<'a, K, V>>>;

    fn insert_async(&'a self, key: K, value: V) -> impl Future<Output = Option<V>>;
}

impl<'a, K, V> DashMapAsync<'a, K, V> for dashmap::DashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone + 'a,
    V: 'a,
{
    async fn entry_async(&'a self, key: K) -> dashmap::mapref::entry::Entry<'_, K, V> {
        std::future::poll_fn(move |_| match self.try_entry(key.clone()) {
            Some(entry) => Poll::Ready(entry),
            None => Poll::Pending,
        })
        .await
    }

    async fn get_async(&'a self, key: &K) -> Option<dashmap::mapref::one::Ref<'_, K, V>> {
        std::future::poll_fn(move |_| match self.try_get(key) {
            dashmap::try_result::TryResult::Present(value) => Poll::Ready(Some(value)),
            dashmap::try_result::TryResult::Absent => Poll::Ready(None),
            dashmap::try_result::TryResult::Locked => Poll::Pending,
        })
        .await
    }

    async fn get_mut_async(&'a self, key: &K) -> Option<dashmap::mapref::one::RefMut<'_, K, V>> {
        std::future::poll_fn(move |_| match self.try_get_mut(key) {
            dashmap::try_result::TryResult::Present(value) => Poll::Ready(Some(value)),
            dashmap::try_result::TryResult::Absent => Poll::Ready(None),
            dashmap::try_result::TryResult::Locked => Poll::Pending,
        })
        .await
    }

    async fn insert_async(&'a self, key: K, value: V) -> Option<V> {
        let mut value = Some(value);
        std::future::poll_fn(|_| match self.try_entry(key.clone()) {
            Some(dashmap::Entry::Vacant(entry)) => {
                let Some(val) = std::mem::take(&mut value) else {
                    return Poll::Ready(None);
                };
                entry.insert_entry(val);
                Poll::Ready(None)
            }
            Some(dashmap::Entry::Occupied(entry)) => {
                let Some(val) = std::mem::take(&mut value) else {
                    return Poll::Ready(None);
                };
                let (_, old) = entry.replace_entry(val);
                Poll::Ready(Some(old))
            }
            None => Poll::Pending,
        })
        .await
    }
}

#[derive(Clone)]
pub struct AsyncDashMap<K: PartialEq + Eq + std::hash::Hash + Clone, V> {
    inner: dashmap::DashMap<K, V>,
}

impl<K, V> Default for AsyncDashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> AsyncDashMap<K, V>
where
    K: std::hash::Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn iter(&self) -> dashmap::iter::Iter<'_, K, V> {
        self.inner.iter()
    }

    pub fn into_iter(self) -> dashmap::iter::OwningIter<K, V> {
        self.inner.into_iter()
    }

    pub async fn entry(&self, key: K) -> dashmap::mapref::entry::Entry<'_, K, V> {
        DashMapAsync::entry_async(&self.inner, key).await
    }

    pub async fn get(&self, key: &K) -> Option<dashmap::mapref::one::Ref<'_, K, V>> {
        DashMapAsync::get_async(&self.inner, key).await
    }

    pub async fn get_mut(&self, key: &K) -> Option<dashmap::mapref::one::RefMut<'_, K, V>> {
        DashMapAsync::get_mut_async(&self.inner, key).await
    }

    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        DashMapAsync::insert_async(&self.inner, key, value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_asyncdashmap_new() {
        let map: AsyncDashMap<i32, i32> = AsyncDashMap::new();
        assert!(map.inner.is_empty(), "Expected the new map to be empty");
    }

    #[tokio::test]
    async fn test_asyncdashmap_insert_and_get() {
        let map = AsyncDashMap::new();
        map.insert(1, 100).await;

        let result = map.get(&1).await;
        assert!(result.is_some(), "Expected to get the value back");
        assert_eq!(*result.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_asyncdashmap_insert_existing_key() {
        let map = AsyncDashMap::new();
        map.insert(1, 100).await;
        let old_value = map.insert(1, 200).await;

        assert_eq!(old_value, Some(100), "Expected old value to be 100");
        let result = map.get(&1).await;
        assert_eq!(*result.unwrap(), 200, "Expected value to be updated to 200");
    }

    #[tokio::test]
    async fn test_asyncdashmap_get_nonexistent_key() {
        let map = AsyncDashMap::<u32, ()>::new();
        let result = map.get(&42).await;
        assert!(result.is_none(), "Expected no value for nonexistent key");
    }

    #[tokio::test]
    async fn test_asyncdashmap_get_mut() {
        let map = AsyncDashMap::new();
        map.insert(1, 100).await;

        {
            let mut value = map.get_mut(&1).await.unwrap();
            *value = 200;
        }

        let result = map.get(&1).await;
        assert_eq!(*result.unwrap(), 200, "Expected value to be updated to 200");
    }
}
