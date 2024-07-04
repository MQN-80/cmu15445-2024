#include "primer/orset.h"
#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  // std::cout<<elem<<""<<E_set.count(elem)<<std::endl;
  if (!e_set_.count(elem)) {
    return false;
  }
  return e_set_.at(elem).size() > 0;
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  e_set_[elem].insert(uid);
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  if (e_set_[elem].size() == 0) {
    return;
  }
  auto ret = e_set_[elem];
  for (int mid : ret) {
    t_set_[elem].insert(mid);
  }
  e_set_.erase(elem);
  // throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  // 1. 通过对面T集合,删除自身E集合
  for (std::pair<T, std::set<int>> mid : other.GetT()) {
    for (int cu : mid.second) {
      if (e_set_[mid.first].count(cu)) {
        e_set_[mid.first].erase(cu);}
    }
  }
  // 2.合并对面E集合,假如E集合不在自己T集合中,则加入
  for (std::pair<T, std::set<int>> mid : other.GetE()) {
    for (int cu : mid.second) {
      if (!t_set_[mid.first].count(cu)){
        e_set_[mid.first].insert(cu);
      } 
    }
  }
  // 3.合并T集合
  for (std::pair<T, std::set<int>> mid : other.GetT()) {
    for (int cu : mid.second) {
      t_set_[mid.first].insert(cu);
    }
  }
  // throw NotImplementedException("ORSet<T>::Merge is not implemented");
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> res;
  for (std::pair<T, std::set<int>> mid : t_set_) {
    res.push_back(mid.first);
  }
  return res;
  // throw NotImplementedException("ORSet<T>::Elements is not implemented");
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
