#include "fstream.h"
#include <vector>
#include <cstring>

// 磁盘事件类型：正常、故障、更换
enum class EventType {
  NORMAL,  // 正常：所有磁盘工作正常
  FAILED,  // 故障：指定磁盘发生故障（文件被删除）
  REPLACED // 更换：指定磁盘被更换（文件被清空）
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_; // 磁盘文件对应的 fstream 对象
  int blocks_per_drive_;               // 每个磁盘的块数
  int block_size_;                     // 每个块的大小
  int num_disks_;                      // 磁盘数
  int failed_drive_;                   // 故障磁盘编号，-1表示无故障

  // 计算第 block_id 个数据块所在的磁盘和磁盘内的块号
  void GetBlockLocation(int block_id, int& disk_id, int& disk_block_id, int& group_id, int& parity_disk) {
    // block_id 是逻辑块号（用户视角）
    // 每组有 (num_disks_ - 1) 个数据块
    group_id = block_id / (num_disks_ - 1);
    int block_in_group = block_id % (num_disks_ - 1);

    // 第 group_id 组的校验块在第 (n-1-group_id) mod n 个磁盘上
    parity_disk = (num_disks_ - 1 - group_id % num_disks_) % num_disks_;

    // 数据块从左到右依次存储在除校验盘外的其他磁盘上
    disk_id = block_in_group;
    if (disk_id >= parity_disk) {
      disk_id++;
    }

    disk_block_id = group_id;
  }

  // 读取指定磁盘的指定块
  void ReadDiskBlock(int disk_id, int disk_block_id, char* buffer) {
    drives_[disk_id]->seekg(disk_block_id * block_size_);
    drives_[disk_id]->read(buffer, block_size_);
  }

  // 写入指定磁盘的指定块
  void WriteDiskBlock(int disk_id, int disk_block_id, const char* buffer) {
    drives_[disk_id]->seekp(disk_block_id * block_size_);
    drives_[disk_id]->write(buffer, block_size_);
  }

  // XOR两个块
  void XorBlocks(char* result, const char* block1, const char* block2) {
    for (int i = 0; i < block_size_; i++) {
      result[i] = block1[i] ^ block2[i];
    }
  }

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096) {
    drives_ = drives;
    blocks_per_drive_ = blocks_per_drive;
    block_size_ = block_size;
    num_disks_ = drives.size();
    failed_drive_ = -1;
  }

  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::NORMAL) {
      failed_drive_ = -1;
    } else if (event_type_ == EventType::FAILED) {
      failed_drive_ = drive_id;
    } else if (event_type_ == EventType::REPLACED) {
      // 恢复被替换的磁盘
      std::vector<char> buffer(block_size_);
      std::vector<char> temp_buffer(block_size_);

      // 遍历每个组
      for (int group_id = 0; group_id < blocks_per_drive_; group_id++) {
        // 计算该组的校验盘
        int parity_disk = (num_disks_ - 1 - group_id % num_disks_) % num_disks_;

        if (drive_id == parity_disk) {
          // 需要恢复校验块
          std::memset(buffer.data(), 0, block_size_);

          // XOR所有数据块
          for (int disk = 0; disk < num_disks_; disk++) {
            if (disk != parity_disk) {
              ReadDiskBlock(disk, group_id, temp_buffer.data());
              XorBlocks(buffer.data(), buffer.data(), temp_buffer.data());
            }
          }

          // 写入恢复的校验块
          WriteDiskBlock(parity_disk, group_id, buffer.data());
        } else {
          // 需要恢复数据块
          // 读取校验块
          ReadDiskBlock(parity_disk, group_id, buffer.data());

          // XOR所有其他数据块
          for (int disk = 0; disk < num_disks_; disk++) {
            if (disk != parity_disk && disk != drive_id) {
              ReadDiskBlock(disk, group_id, temp_buffer.data());
              XorBlocks(buffer.data(), buffer.data(), temp_buffer.data());
            }
          }

          // 写入恢复的数据块
          WriteDiskBlock(drive_id, group_id, buffer.data());
        }
      }

      // 如果之前该磁盘是故障的，现在已经恢复
      if (failed_drive_ == drive_id) {
        failed_drive_ = -1;
      }
    }
  }

  void Shutdown() {
    for (auto drive : drives_) {
      if (drive && drive->is_open()) {
        drive->close();
      }
    }
  }

  void ReadBlock(int block_id, char *result) {
    int disk_id, disk_block_id, group_id, parity_disk;
    GetBlockLocation(block_id, disk_id, disk_block_id, group_id, parity_disk);

    if (failed_drive_ == -1 || failed_drive_ != disk_id) {
      // 正常读取
      ReadDiskBlock(disk_id, disk_block_id, result);
    } else {
      // 降级模式：需要通过XOR恢复数据
      std::vector<char> temp_buffer(block_size_);

      // 读取校验块
      ReadDiskBlock(parity_disk, disk_block_id, result);

      // XOR所有其他数据块
      for (int disk = 0; disk < num_disks_; disk++) {
        if (disk != parity_disk && disk != failed_drive_) {
          ReadDiskBlock(disk, disk_block_id, temp_buffer.data());
          XorBlocks(result, result, temp_buffer.data());
        }
      }
    }
  }

  void WriteBlock(int block_id, const char *data) {
    int disk_id, disk_block_id, group_id, parity_disk;
    GetBlockLocation(block_id, disk_id, disk_block_id, group_id, parity_disk);

    std::vector<char> old_data(block_size_);
    std::vector<char> old_parity(block_size_);
    std::vector<char> new_parity(block_size_);

    if (failed_drive_ == -1) {
      // 正常模式：读取旧数据和旧校验，计算新校验
      if (disk_id != failed_drive_) {
        ReadDiskBlock(disk_id, disk_block_id, old_data.data());
      }
      ReadDiskBlock(parity_disk, disk_block_id, old_parity.data());

      // 新校验 = 旧校验 XOR 旧数据 XOR 新数据
      XorBlocks(new_parity.data(), old_parity.data(), old_data.data());
      XorBlocks(new_parity.data(), new_parity.data(), data);

      // 写入新数据和新校验
      WriteDiskBlock(disk_id, disk_block_id, data);
      WriteDiskBlock(parity_disk, disk_block_id, new_parity.data());
    } else {
      // 降级模式
      if (disk_id == failed_drive_) {
        // 要写入的数据块在故障磁盘上，需要更新校验块
        // 新校验 = XOR(所有其他数据块) XOR 新数据
        std::memset(new_parity.data(), 0, block_size_);
        XorBlocks(new_parity.data(), new_parity.data(), data);

        std::vector<char> temp_buffer(block_size_);
        for (int disk = 0; disk < num_disks_; disk++) {
          if (disk != parity_disk && disk != failed_drive_) {
            ReadDiskBlock(disk, disk_block_id, temp_buffer.data());
            XorBlocks(new_parity.data(), new_parity.data(), temp_buffer.data());
          }
        }

        WriteDiskBlock(parity_disk, disk_block_id, new_parity.data());
      } else if (parity_disk == failed_drive_) {
        // 校验块在故障磁盘上，只需写入数据块
        WriteDiskBlock(disk_id, disk_block_id, data);
      } else {
        // 数据块和校验块都不在故障磁盘上
        ReadDiskBlock(disk_id, disk_block_id, old_data.data());
        ReadDiskBlock(parity_disk, disk_block_id, old_parity.data());

        XorBlocks(new_parity.data(), old_parity.data(), old_data.data());
        XorBlocks(new_parity.data(), new_parity.data(), data);

        WriteDiskBlock(disk_id, disk_block_id, data);
        WriteDiskBlock(parity_disk, disk_block_id, new_parity.data());
      }
    }
  }

  int Capacity() {
    return (num_disks_ - 1) * blocks_per_drive_;
  }
};
