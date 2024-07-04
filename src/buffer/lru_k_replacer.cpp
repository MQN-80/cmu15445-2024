//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}
LRUKReplacer::~LRUKReplacer(){
    for(auto mid:node_store_){
        delete mid.second;
        mid.second=nullptr;
    }
    node_store_.clear();
}
//驱逐页面
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex>lk(latch_);
    //1.首先驱逐history里的page
    for(auto mid:history_list){
        LRUKNode* node=node_store_[mid.second];
        if(node->getIs_evictable_()==false)continue;
        history_list.erase(mid.first);
        node_store_.erase(mid.second);
        *frame_id=mid.second;
        if(node->getIs_evictable_()==true)curr_size_--;
        delete node;
        return true;
    }
    //2.假如没有则驱逐缓存队列里的page
    for(auto mid:cache_list){
        LRUKNode* node=node_store_[mid.second];
        if(node->getIs_evictable_()==false)continue;
        cache_list.erase(mid.first);
        node_store_.erase(mid.second);
        *frame_id=mid.second;
        if(node->getIs_evictable_()==true)curr_size_--;
        delete node;
        return true;
    }
    return false; 
}
//进行访问
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex>lk(latch_);
    //1.首先查看其是否为第一次访问
    if(!node_store_.count(frame_id)){
        LRUKNode* node=new LRUKNode(k_,++current_timestamp_,false);
        node_store_[frame_id]=node;
        //node_store_.emplace(frame_id,std::move(node));
        history_list.insert(std::make_pair(current_timestamp_,frame_id));
        return;
    }
    //2.假如不是第1次访问,看该节点是在history里还是cache里
    LRUKNode* node=node_store_[frame_id];
    size_t pre_time=node->getHistory().back(); //最近一次访问时间
    if(history_list.count(pre_time)){
        node->addLooks();   //增加访问次数
        if(node->getLooks()==k_){
            history_list.erase(pre_time);
            node->pushTime(++current_timestamp_);
            cache_list.insert(std::make_pair(node->getKtime(),frame_id));
        }
        else{
            node->pushTime(++current_timestamp_);
            history_list.erase(pre_time);
            history_list.insert(std::make_pair(current_timestamp_,frame_id));
        }
    }
    else{
        pre_time=node->getKtime(); //第k次访问时间
        node->pushTime(++current_timestamp_);
        cache_list.erase(pre_time);
        cache_list.insert(std::make_pair(node->getKtime(),frame_id));
    }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex>lk(latch_);
    if(!node_store_.count(frame_id))return;
    LRUKNode* node=node_store_[frame_id];
    if(node->getIs_evictable_()&&!set_evictable){
        curr_size_--;
    }
    if(set_evictable)curr_size_++;
    node->setIs_evictable_(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex>lk(latch_);
    if(!node_store_.count(frame_id))return;
    LRUKNode* node=node_store_[frame_id];
    size_t last_time=node->getHistory().back();
    size_t k_time=node->getKtime();
    history_list.erase(last_time);
    cache_list.erase(k_time);  
}

auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex>lk(latch_);
    return curr_size_; 
}

}  // namespace bustub
