#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    page_=that.page_;
    bpm_=that.bpm_;
    is_dirty_=that.is_dirty_;
    that.page_=nullptr;
    that.bpm_=nullptr;
    //that=nullptr;
}

void BasicPageGuard::Drop() {
    if(page_!=nullptr){
        bpm_->UnpinPage(page_->GetPageId(),is_dirty_);
    }
    page_=nullptr;
    is_dirty_=false;
    bpm_=nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    this->Drop();
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;
    this->page_ = that.page_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    if(page_!=nullptr)this->Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
    this->page_->RLatch();   //获取写锁
    ReadPageGuard read_guard(this->bpm_, this->page_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
    return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
    this->page_->WLatch();
    WritePageGuard write_guard(this->bpm_, this->page_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
    return write_guard;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
    //BasicPageGuard guard(bpm,page);
    this->guard_=std::make_unique<BasicPageGuard>(bpm,page);
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept{
    this->guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    if(guard_==nullptr)return;
    if(this->guard_->page_!=nullptr){
        guard_->bpm_->UnpinPage(this->PageId(), this->guard_->is_dirty_);
        guard_->page_->RUnlatch();
    }
    guard_->page_ = nullptr;
    guard_->bpm_ = nullptr;
    guard_->is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {
    //assert(guard_!=nullptr);
    //if(guard_->page_==nullptr)
    if(guard_!=nullptr&&guard_->page_!=nullptr)this->Drop();

}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
    this->guard_=std::make_unique<BasicPageGuard>(bpm,page);
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
    this->guard_=std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    if(guard_==nullptr)return;
    if(this->guard_->page_!=nullptr){
        guard_->bpm_->UnpinPage(this->PageId(), this->guard_->is_dirty_);
        guard_->page_->RUnlatch();
    }
    guard_->page_ = nullptr;
    guard_->bpm_ = nullptr;
    guard_->is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() {
    if(guard_!=nullptr&&guard_->page_!=nullptr)this->Drop();
}  // NOLINT

}  // namespace bustub
