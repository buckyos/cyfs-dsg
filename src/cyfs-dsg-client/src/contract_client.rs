use std::{convert::TryFrom, fmt::Debug, sync::Arc};
use std::str::FromStr;
use cyfs_base::*;
use cyfs_lib::*;
use crate::{contracts::*, DSGJSON, DsgJSONObject, DsgJsonProtocol, query::*, RecoveryReq, RecoveryState};
use crate::shared_cyfs_stack_ex::{CyfsPath, SharedCyfsStackEx};

pub struct DsgClientInterface<T>
where
    T: 'static + Send + Sync + for<'de> RawDecode<'de> + RawEncode + Clone + Debug,
{
    stack: Arc<SharedCyfsStack>,
    _reserved: std::marker::PhantomData<T>,
}

impl<T> DsgClientInterface<T>
where
    T: 'static + Send + Sync + for<'de> RawDecode<'de> + RawEncode + Clone + Debug,
{
    fn new(stack: Arc<SharedCyfsStack>) -> Self {
        Self {
            stack,
            _reserved: Default::default(),
        }
    }

    fn stack(&self) -> &SharedCyfsStack {
        self.stack.as_ref()
    }

    async fn apply_contract<'a>(&self, contract: DsgContractObjectRef<'a, T>) -> BuckyResult<()> {
        log::info!("DsgClient will apply contract, contract={}", contract);
        self.put_object_to_noc(contract.id(), contract.as_ref())
            .await
            .map_err(|err| {
                log::error!(
                    "DsgClient apply contract failed, id={}, err=put to noc {}",
                    contract.id(),
                    err
                );
                err
            })?;

        log::info!("DsgClient apply contract finished, id={}", contract.id());
        Ok(())
    }

    pub async fn sync_contract_state(
        &self,
        new_state: &DsgContractStateObject,
    ) -> BuckyResult<DsgContractStateObject> {
        let state_ref = DsgContractStateObjectRef::from(new_state);
        log::info!("DsgClient try sync contract state, state={}", state_ref);

        let path = RequestGlobalStatePath::new(Some(dsg_dec_id()), Some("/dsg/service/sync/state/")).format_string();
        log::info!("sync contract state req_path: {}", &path);
        let mut req = NONPostObjectOutputRequest::new(
            NONAPILevel::default(),
            DsgContractStateObjectRef::from(new_state).id(),
            new_state.to_vec()?,
        );
        req.common.req_path = Some(path);

        let resp = self
            .stack()
            .non_service()
            .post_object(req)
            .await
            .map_err(|err| {
                log::error!(
                    "DsgClient sync contract state failed, contract={}, state={}, err={}",
                    state_ref.contract_id(),
                    state_ref.id(),
                    err
                );
                err
            })?;
        let resp = resp.object.unwrap();
        let cur_state = DsgContractStateObject::clone_from_slice(resp.object_raw.as_slice())
            .map_err(|err| {
                log::error!("DsgClient sync contract state failed, contract={}, state={}, err=decode resp {}", state_ref.contract_id(), state_ref.id(), err);
                err
            })?;
        let cur_state_ref = DsgContractStateObjectRef::from(&cur_state);
        if cur_state_ref != state_ref {
            log::error!(
                "DsgClient sync contract state mismatch, contract={}, state={}, cur_state={}",
                state_ref.contract_id(),
                state_ref.id(),
                cur_state_ref
            );
        } else {
            log::info!(
                "DsgClient sync contract state sucess, contract={}, state={}",
                state_ref.contract_id(),
                state_ref.id()
            );
        }
        Ok(cur_state)
    }

    pub async fn query(&self, query: DsgQuery) -> BuckyResult<DsgQuery> {
        let query_obj: DsgQueryObject = query.into();

        let path = RequestGlobalStatePath::new(Some(dsg_dec_id()), Some("/dsg/service/query/")).format_string();
        let mut req = NONPostObjectOutputRequest::new(
            NONAPILevel::default(),
            query_obj.desc().object_id(),
            query_obj.to_vec()?,
        );
        req.common.req_path = Some(path);


        let resp = self
            .stack()
            .non_service()
            .post_object(req)
            .await?;
        let resp = resp.object.unwrap();
        let resp_obj = DsgQueryObject::clone_from_slice(resp.object_raw.as_slice())?;
        DsgQuery::try_from(resp_obj)
    }

    #[tracing::instrument(skip(self), ret, err)]
    pub async fn recovery_contract(&self, contract_id: &ObjectId, latest_state_id: &ObjectId, target_id: &ObjectId, target_dec_id: &ObjectId) -> BuckyResult<String> {
        let req = RecoveryReq {
            contract_id: contract_id.to_string(),
            latest_state_id: latest_state_id.to_string(),
            target_id: target_id.to_string(),
            target_dec_id: target_dec_id.to_string()
        };
        let dec_id = self.stack.dec_id().unwrap().clone();
        let owner_id = self.stack.local_device().desc().owner().as_ref().unwrap().clone();
        let local_id = self.stack.local_device_id().object_id().clone();
        let req = DsgJSONObject::new(dec_id, owner_id, DsgJsonProtocol::Recovery as u16, &req)?;
        let req_path = CyfsPath::new(local_id, dsg_dec_id(), "/dsg/service/commands/").to_path();
        let resp: DsgJSONObject = self.stack.put_object_with_resp2(req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        resp.get()
    }

    pub async fn query_recovery_state(&self, task_id: String) -> BuckyResult<RecoveryState> {
        let dec_id = self.stack.dec_id().unwrap().clone();
        let owner_id = self.stack.local_device().desc().owner().as_ref().unwrap().clone();
        let local_id = self.stack.local_device_id().object_id().clone();
        let req = DsgJSONObject::new(dec_id, owner_id, DsgJsonProtocol::QueryRecoveryState as u16, &task_id)?;
        let req_path = CyfsPath::new(local_id, dsg_dec_id(), "/dsg/service/commands/").to_path();
        let resp: DsgJSONObject = self.stack.put_object_with_resp2(req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        resp.get()
    }

    pub async fn revert(&self, state_id: &ObjectId) -> BuckyResult<ObjectId> {
        let dec_id = self.stack.dec_id().unwrap().clone();
        let owner_id = self.stack.local_device().desc().owner().as_ref().unwrap().clone();
        let local_id = self.stack.local_device_id().object_id().clone();
        let req = DsgJSONObject::new(dec_id, owner_id, DsgJsonProtocol::Revert as u16, &state_id.to_string())?;
        let req_path = CyfsPath::new(local_id, dsg_dec_id(), "/dsg/service/commands/").to_path();
        let resp: DsgJSONObject = self.stack.put_object_with_resp2(req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        let revert_id: String = resp.get()?;
        Ok(ObjectId::from_str(revert_id.as_str())?)
    }

    pub async fn get_object_from_noc<O: for<'de> RawDecode<'de>>(
        &self,
        id: ObjectId,
    ) -> BuckyResult<O> {
        let resp = self
            .stack()
            .non_service()
            .get_object(NONGetObjectOutputRequest::new(NONAPILevel::NOC, id, None))
            .await?;
        O::clone_from_slice(resp.object.object_raw.as_slice())
    }

    async fn put_object_to_noc<O: RawEncode>(&self, id: ObjectId, object: &O) -> BuckyResult<()> {
        let mut req = NONPutObjectOutputRequest::new(
            NONAPILevel::NOC,
            id,
            object.to_vec()?,
        );
        req.access = Some(AccessString::full());
        let _ = self
            .stack()
            .non_service()
            .put_object(req)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait DsgClientDelegate: Send + Sync {
    type Witness: 'static + Send + Sync + for<'de> RawDecode<'de> + RawEncode + Clone + Debug;
    fn dec_id(&self) -> &ObjectId;
    async fn add_contract(&self, id: &ObjectId) -> BuckyResult<()>;
    async fn remove_contract(&self, id: &ObjectId) -> BuckyResult<()>;
}

struct ClientImpl<D>
where
    D: 'static + DsgClientDelegate,
{
    interface: DsgClientInterface<D::Witness>,
    delegate: D,
}

pub struct DsgClient<D>
where
    D: 'static + DsgClientDelegate,
{
    inner: Arc<ClientImpl<D>>,
}

impl<D> Clone for DsgClient<D>
where
    D: 'static + DsgClientDelegate,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D> DsgClient<D>
where
    D: 'static + DsgClientDelegate,
{
    pub fn new(stack: Arc<SharedCyfsStack>, delegate: D) -> BuckyResult<Self> {
        let client = Self {
            inner: Arc::new(ClientImpl {
                interface: DsgClientInterface::new(stack),
                delegate,
            }),
        };

        Ok(client)
    }

    pub fn interface(&self) -> &DsgClientInterface<D::Witness> {
        &self.inner.interface
    }

    pub fn delegate(&self) -> &D {
        &self.inner.delegate
    }

    pub async fn apply_contract<'a>(
        &self,
        contract: DsgContractObjectRef<'a, D::Witness>,
    ) -> BuckyResult<()> {
        let _ = self.interface().apply_contract(contract.clone()).await?;
        let _ = self.delegate().add_contract(&contract.id()).await?;
        Ok(())
    }

    pub async fn remove_contract(&self, contract: &ObjectId) -> BuckyResult<()> {
        self.delegate().remove_contract(contract).await
    }
}
