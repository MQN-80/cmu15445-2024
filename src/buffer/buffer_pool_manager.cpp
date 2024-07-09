//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  //1.首先从空闲队列里寻找,假如没有则利用lru-k
  std::lock_guard<std::mutex>lk(latch_);
  Page* page;
  frame_id_t cur_frame=-1;
  if(!free_list_.empty()){
    cur_frame=free_list_.back();
    free_list_.pop_back();
    page=pages_+cur_frame;
  }
  else{
    if(!replacer_->Evict(&cur_frame))return nullptr;
    page=pages_+cur_frame;
  }
  //假如拿出的页是脏的,将其写会到磁盘
  if(page->is_dirty_){
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({true,page->GetData(),page->GetPageId(),std::move(promise)});
    future.get();
    page->is_dirty_=false;
  }
  //获取新页面ID
  *page_id=AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_.emplace(*page_id,cur_frame);
  page->page_id_=*page_id;
  page->pin_count_=1;
  page->ResetMemory();
  replacer_->RecordAccess(cur_frame);
  replacer_->SetEvictable(cur_frame,false);
  return page; 
  }

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex>lk(latch_);
  //先在缓冲池内寻找
  if(page_table_.count(page_id)){
    auto frame_id=page_table_[page_id];
    auto page=pages_+frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id,false);
    page->pin_count_++;
    return page;
  }
  //找不到的话同newPage
  Page* page;
  frame_id_t cur_frame=-1;
  if(!free_list_.empty()){
    cur_frame=free_list_.back();
    free_list_.pop_back();
    page=pages_+cur_frame;
  }
  else{
    if(!replacer_->Evict(&cur_frame))return nullptr;
    page=pages_+cur_frame;
  }
  //假如拿出的页是脏的,将其写会到磁盘
  if(page->is_dirty_){
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({true,page->GetData(),page->GetPageId(),std::move(promise)});
    future.get();
    page->is_dirty_=false;
  }
  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id,cur_frame);
  page->page_id_=page_id;
  page->pin_count_=1;
  page->ResetMemory();
  //从磁盘中读页
  auto promise=disk_scheduler_->CreatePromise();
  auto future=promise.get_future();
  disk_scheduler_->Schedule({false,page->GetData(),page->GetPageId(),std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex>lk(latch_);
  if(page_id==INVALID_PAGE_ID)return false;
  if(!page_table_.count(page_id))return false;
  auto frame_id=page_table_[page_id];
  auto page=pages_+frame_id;
  page->is_dirty_=is_dirty||page->is_dirty_;
  if(page->GetPinCount()==0)return false;
  page->pin_count_--;
  if(page->GetPinCount()==0)replacer_->SetEvictable(frame_id,true);
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex>lk(latch_);
  if(page_id==INVALID_PAGE_ID)return false;
  if(!page_table_.count(page_id))return false;
  auto frame_id=page_table_[page_id];
  auto page=pages_+frame_id;
  auto promise=disk_scheduler_->CreatePromise();
  auto future=promise.get_future();
  disk_scheduler_->Schedule({true,page->GetData(),page->GetPageId(),std::move(promise)});
  future.get();
  page->is_dirty_=false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex>lk(latch_);
  for (size_t current_size = 0; current_size < pool_size_; current_size++) {
    // 获得page_id在缓冲池中的位置
    auto page = pages_ + current_size;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex>lk(latch_);
  if(page_table_.count(page_id)){
    auto frame_id=page_table_[page_id];
    auto page=pages_+frame_id;
    if(page->GetPinCount()>0)return false;
    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }
  DeallocatePage(page_id);
  return true;
  

 }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  auto page=FetchPage(page_id);
  return {this, page}; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  auto page=FetchPage(page_id);
  page->RLatch();
  return {this, page}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  auto page=FetchPage(page_id);
  page->WLatch();
  return {this, page}; 
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
