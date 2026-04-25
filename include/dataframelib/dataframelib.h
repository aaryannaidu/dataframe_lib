#pragma once

#include <arrow/result.h>
#include <arrow/status.h>

// Arrow 23 removed ARROW_THROW_NOT_OK; provide it for compatibility.
#ifndef ARROW_THROW_NOT_OK
#define ARROW_THROW_NOT_OK(expr)                                    \
    do {                                                            \
        auto _s = (expr);                                           \
        if (!_s.ok()) throw std::runtime_error(_s.ToString());      \
    } while (false)
#endif

#include "../Expression.hpp"
#include "../EagerDataFrame.hpp"
#include "../LazyDataFrame.hpp"
