
#include "service.h"
#include "server.h"
#include "../paxos_worker.h"

namespace janus {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(TxLogServer *sched)
    : sched_((PaxosServer*)sched) {

}

void MultiPaxosServiceImpl::Forward(const MarshallDeputy& cmd,
                                    rrr::DeferredReply* defer) {
}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    ballot_t* max_ballot,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnPrepare(slot,
                    ballot,
                    max_ballot,
                    std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Accept(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   ballot_t* max_ballot,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnAccept(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                     max_ballot,
                     std::bind(&rrr::DeferredReply::reply, defer));

  });
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnCommit(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_);
    defer->reply();
  });
}


void MultiPaxosServiceImpl::BulkPrepare(const MarshallDeputy& md_cmd,
                                       i32* ballot,
                                       i32* valid,
                                       rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    //std::cout << "send a BulkPrepare\n";
    sched_->OnBulkPrepare(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                          ballot,
                          valid,
                          std::bind(&rrr::DeferredReply::reply, defer));
  });
}

//marker:ansh complete, basic skeleton, add rpc definition in rcc_rpc.rpc
void MultiPaxosServiceImpl::Heartbeat(const MarshallDeputy& md_cmd,
                                       i32* ballot,
                                       i32* valid,
                                       rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnHeartbeat(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                          ballot,
                          valid,
                          std::bind(&rrr::DeferredReply::reply, defer));
  });
}

void MultiPaxosServiceImpl::BulkPrepare2(const MarshallDeputy& md_cmd,
                                       i32* ballot,
                                       i32* valid,
                                       MarshallDeputy* ret,
                                       rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  ret->SetMarshallable(std::make_shared<BulkPaxosCmd>());
  auto p = dynamic_pointer_cast<BulkPaxosCmd>(ret->sp_data_);
  //Log_info("The marshallable flag is %d", p->bypass_to_socket_);
  Coroutine::CreateRun([&] () {
    sched_->OnBulkPrepare2(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                          ballot,
                          valid,
                          p,
                          std::bind(&rrr::DeferredReply::reply, defer));
  });
}

void MultiPaxosServiceImpl::BulkAccept(const MarshallDeputy& md_cmd,
                                       i32* ballot,
                                       i32* valid,
                                       rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnBulkAccept(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                         ballot,
                         valid,
                        std::bind(&rrr::DeferredReply::reply, defer));
  });
}

void MultiPaxosServiceImpl::BulkDecide(const MarshallDeputy& md_cmd,
                                       i32* ballot,
                                       i32* valid,
                                       rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnBulkCommit(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                         ballot,
                         valid,
                         std::bind(&rrr::DeferredReply::reply, defer));
    //defer->reply();
  });
}

void MultiPaxosServiceImpl::SyncLog(const MarshallDeputy& md_cmd,
                                     i32* ballot,
                                     i32* valid,
                                     MarshallDeputy* ret,
                                     rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  ret->SetMarshallable(std::make_shared<SyncLogResponse>());
  auto response = dynamic_pointer_cast<SyncLogResponse>(ret->sp_data_);
  Coroutine::CreateRun([&] () {
    sched_->OnSyncLog(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                      ballot,
                      valid,
                      response,
                      std::bind(&rrr::DeferredReply::reply, defer));
  //  auto rpx = dynamic_pointer_cast<SyncLogResponse>(ret->sp_data_);
  //   auto xx = (int32_t)rpx->missing_slots.size();
  //   Log_info("received a OnSyncLog2,xxx: %d",xx);
  //   for(int i = 0; i < rpx->missing_slots.size(); i++){
  //      Log_info("yy2: %d", (int32_t)rpx->missing_slots[i].size());
  //      for(int j = 0; j < rpx->missing_slots[i].size(); j++){
  //         Log_info("yy2 a OnSyncLog2,xxx: %d",j);
  //      }
  //   }
    defer->reply();
  });

}

void MultiPaxosServiceImpl::SyncCommit(const MarshallDeputy& md_cmd,
                                     i32* ballot,
                                     i32* valid,
                                     rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnSyncCommit(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                         ballot,
                         valid,
                         std::bind(&rrr::DeferredReply::reply, defer));
    //defer->reply();
  });
}

void MultiPaxosServiceImpl::SyncNoOps(const MarshallDeputy& md_cmd,
                                      i32* ballot,
                                      i32* valid,
                                      rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnSyncNoOps(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                         ballot,
                         valid,
                         std::bind(&rrr::DeferredReply::reply, defer));
    //defer->reply();
  });
}

void MultiPaxosServiceImpl::ForwardToLearnerServer(const rrr::i32& par_id,
                                                   const uint64_t& slot, 
                                                   const ballot_t& ballot, /* slot and ballot from the leader */
                                                   const MarshallDeputy& cmd, 
                                                   uint64_t* ret_slot, ballot_t* ret_ballot, rrr::DeferredReply* defer) {
    verify(sched_ != nullptr);
    *ret_slot = slot;
    *ret_ballot = ballot;
    Coroutine::CreateRun([&] () {
      sched_->OnForwardToLearner(par_id, slot, ballot, const_cast<MarshallDeputy&>(cmd).sp_data_,
                               std::bind(&rrr::DeferredReply::reply, defer));
    });
}


} // namespace janus;
