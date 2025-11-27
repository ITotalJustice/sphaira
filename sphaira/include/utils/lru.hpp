#pragma once

#include <vector>
#include <span>

namespace sphaira::utils {

template<typename T>
struct LinkedList {
    T* data;
    LinkedList* next;
    LinkedList* prev;
};

template<typename T>
struct Lru {
    using ListEntry = LinkedList<T>;

    // pass span of the data.
    void Init(std::span<T> data) {
        list_flat_array.clear();
        list_flat_array.resize(data.size());

        auto list_entry = list_head = list_flat_array.data();

        for (size_t i = 0; i < data.size(); i++) {
            list_entry = list_flat_array.data() + i;
            list_entry->data = data.data() + i;

            if (i + 1 < data.size()) {
                list_entry->next = &list_flat_array[i + 1];
            }
            if (i) {
                list_entry->prev = &list_flat_array[i - 1];
            }
        }

        list_tail = list_entry->prev->next;
    }

    // moves entry to the front of the list.
    void Update(ListEntry* entry) {
        // only update position if we are not the head.
        if (list_head != entry) {
            entry->prev->next = entry->next;
            if (entry->next) {
                entry->next->prev = entry->prev;
            } else {
                list_tail = entry->prev;
            }

            // update head.
            auto head_temp = list_head;
            list_head = entry;
            list_head->prev = nullptr;
            list_head->next = head_temp;
            head_temp->prev = list_head;
        }
    }

    // moves last entry (tail) to the front of the list.
    auto GetNextFree() {
        Update(list_tail);
        return list_head->data;
    }

    auto begin() const { return list_head; }
    auto end() const { return list_tail; }

private:
    ListEntry* list_head{};
    ListEntry* list_tail{};
    std::vector<ListEntry> list_flat_array{};
};

} // namespace sphaira::utils
