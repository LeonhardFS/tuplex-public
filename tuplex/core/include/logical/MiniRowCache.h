//
// Created by leonhards on 8/17/24.
//

#ifndef TUPLEX_MINIROWCACHE_H
#define TUPLEX_MINIROWCACHE_H

#include <Row.h>

namespace tuplex {
    /*!
     * helper class to store rows for operators to avoid expensive recompute.
     * For now this is a lightweight wrapper, can expand in the future.
     */
    class MiniRowCache {
    public:

        inline void set(const std::vector<Row>& rows) {
            _rows = rows;
        }
        inline std::vector<Row> cached_rows(size_t n) {
            return std::vector<Row>(_rows.begin(), _rows.begin() + std::min(n, _rows.size()));
        }
        size_t count() const { return _rows.size(); }
    private:
        std::vector<Row> _rows;
    };
}
#endif //TUPLEX_MINIROWCACHE_H
