//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include <set>
#include <queue>
#include <map>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::queue<size_t> history_;   //存储历次访问的时间戳,size最大为k_
  size_t k_;   
  frame_id_t fid_;    //对应的frame_id
  bool is_evictable_{false};   //是否能驱逐
  size_t looks{1}; //被访问的次数
  size_t k_time; //距离第k次时间戳
public:
  bool getIs_evictable_(){
    return is_evictable_;
  }
  void setIs_evictable_(bool flag){
    is_evictable_=flag;
  }
  size_t getLooks(){
    return looks;
  }
  void addLooks(){
    looks++;
  }
  auto getHistory()->std::queue<size_t>{
    return history_;
  }
  auto getKtime()->size_t{
    return k_time;
  }
  /**
   * @brief 加入时间戳,假如history已经等于k_,则先弹出一个队顶元素再加入
   */
  void pushTime(size_t timestamp){
    if(history_.size()==k_){
      history_.pop();
    }
    history_.push(timestamp);
    k_time=history_.front();
  }
  LRUKNode(int _k,frame_id_t _fid,bool _isev):k_(_k),fid_(_fid),is_evictable_(_isev){
    pushTime(_fid);
  }
  LRUKNode(){}
  
};
/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode*> node_store_;   //存储frame_id_t对应的node
  size_t current_timestamp_{0};    //当前时间戳,从0开始
  size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  //using k_time=std::pair<frame_id_t,size_t>;   //对应node倒数第k次的时间戳
  std::map<size_t,frame_id_t>history_list; //不满k次的历史队列<时间戳,帧序号>
  //std::unordered_map<size_t, frame_id_t> history_store_;
  //std::set<size_t>cache_list; //达到k次的缓存队列,按时间戳从小到大排列
  std::map<size_t,frame_id_t>cache_list;//达到k次的缓存队列,按时间戳从小到大排列
  //std::unordered_map<size_t, frame_id_t> cache_store_;
};

}  // namespace bustub
