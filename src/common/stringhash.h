// Author: Keonwoo Oh (koh3@umd.edu)

# ifndef STRING_HASH_H
# define STRING_HASH_H

struct string_hash {
  using is_transparent = void;
  [[nodiscard]] size_t operator()(const char *txt) const {
    return std::hash<std::string_view>{}(txt);
  }
  [[nodiscard]] size_t operator()(std::string_view txt) const {
    return std::hash<std::string_view>{}(txt);
  }
  [[nodiscard]] size_t operator()(const std::string &txt) const {
    return std::hash<std::string>{}(txt);
  }
};

#endif