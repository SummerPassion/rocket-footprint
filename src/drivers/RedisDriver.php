<?php
declare (strict_types=1);

/**
 * Author:
 *
 *   ┏┛ ┻━━━━━┛ ┻┓
 *   ┃　　　━　　  ┃
 *   ┃　┳┛　  ┗┳  ┃
 *   ┃　　　-　　  ┃
 *   ┗━┓　　　┏━━━┛
 *     ┃　　　┗━━━━━━━━━┓
 *     ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
 *       ┗━┻━┛   ┗━┻━┛
 * DateTime: 2022-02-25 19:13:59
 */

namespace rocket_footprint\drivers;

use rocket_footprint\contracts\Driver;
use rocket_footprint\Utils;
use think\facade\Config;

/**
 * redis 实现
 * Class RedisDriver
 * @package rocket\drivers
 * create_at: 2022-02-25 19:14:23
 * update_at: 2022-02-25 19:14:23
 */
class RedisDriver extends Driver
{
    /**
     * redis实例
     * @var object
     */
    private $redis_obj = null;

    /**
     * 配置
     * @var array|mixed
     */
    protected $fp_config = [];

    /**
     * RedisDriver constructor.
     */
    public function __construct()
    {
        $this->redis_obj = Utils::redis();
        $this->fp_config = Config::get('footprint');
    }

    /**
     * 记录
     * @param string $val 记录值
     * @param string $uid 用户标识
     * @param string|null $ord 排序
     * @param string|null $env 场景
     * @return bool|mixed
     */
    public function log($val, $uid = null, $ord = self::SEQ, $env = self::DEFAULT, $step = 1)
    {
        if (!$val && !is_numeric($val)) {
            throw new \InvalidArgumentException("记录值不能为空。");
        }

        $max_len = $this->fp_config['max_len'] ?? $this->max_len;

        if (mb_strlen($val, 'UTF-8') > $max_len) {
            throw new \InvalidArgumentException("记录值长度不能大于{$max_len}个中英文字符。");
        }

        $key = self::$fp_prefix . ($env ?: self::DEFAULT) . ':' . ($uid ? self::$user_prefix . $uid : '');

        switch ($ord) {
            case self::HEAT:
                $this->logByHeat($key, $val, $step);
                break;
            case self::SEQ:
                $this->logBySeq($key, $val);
                break;
            default:
                throw new \InvalidArgumentException("不支持的排序参数。");
        }

        return true;
    }

    /**
     * 记录热度历史
     * @param $key
     * @param $val
     */
    protected function logByHeat($key, $val, $step = 1)
    {
        // 原子操作
        if ($this->redis_obj->zIncrby($key, $step, $val)) {

            // 超限判断
            $fp_config = $this->fp_config['heat'];

            if ($this->redis_obj->zCard($key) > $fp_config['persist']) {
                if (!$this->redis_obj->zRemRangeByRank($key, 0, $fp_config['del'])) {
                    throw new \RuntimeException('清除历史信息异常！');
                }
            }
        } else {
            throw new \RuntimeException('记录热度排名历史异常！');
        }
    }

    /**
     * 记录时序历史
     * @param $key
     * @param $val
     */
    protected function logBySeq($key, $val)
    {
        if (false !== $pos = mb_strpos($val, '_')) {
            $rem_val = mb_substr($val, 0, mb_strpos($val, '_'));
            $list = $this->redis_obj->lRange($key, 0, -1);
            foreach ($list as $k => $v) {
                $v_sub = mb_substr($v, 0, mb_strpos($v, '_'));
                if ($rem_val == $v_sub) {
                    $this->redis_obj->lRem($key, 0, $v);
                }
            }
        }

        $this->redis_obj->lRem($key, 0, $val);
        if (false === $this->redis_obj->lPush($key, $val)) {
            throw new \RuntimeException('记录时序排名历史异常！');
        }

        // 超限判断
        $fp_config = $this->fp_config['seq'];

        if ($this->redis_obj->lLen($key) > $fp_config['persist']) {
            $this->redis_obj->lTrim($key, 0, ($fp_config['persist'] - $fp_config['del']));
        }
    }

    /**
     * 获取历史
     * @param int $end 长度
     * @param null $uid 用户标识
     * @param string $ord 排序
     * @param string $env 场景
     * @return mixed|void
     */
    public function get($end, $uid = null, $ord = self::SEQ, $env = self::DEFAULT)
    {
        $key = self::$fp_prefix . ($env ?: self::DEFAULT) . ':' . ($uid ? self::$user_prefix . $uid : '');

        switch ($ord) {
            case self::HEAT:
                $method = "zRevRange";
                break;
            case self::SEQ:
                $method = "lRange";
                break;
            default:
                throw new \RuntimeException("不支持的排序参数。");
        }

        return $this->redis_obj->{$method}($key, 0, $end - 1);
    }

    /**
     * 获取list长度
     * @param int $end 长度
     * @param null $uid 用户标识
     * @param string $ord 排序
     * @param string $env 场景
     * @return mixed|void
     */
    public function getListLen($uid = null, $env = self::DEFAULT, $ord = self::SEQ)
    {
        $key = self::$fp_prefix . ($env ?: self::DEFAULT) . ':' . ($uid ? self::$user_prefix . $uid : '');

        return $this->redis_obj->lLen($key);
    }

    /**
     * 分页获取
     * @param int $uid 用户id
     * @param int $page 第N页
     * @param int $pageSize 页面大小
     * @param string $env 场景
     * @param string $ord 排序
     */
    public function pageQuery($uid, $page = self::PAGE, $pageSize = self::PAGESIZE, $env = self::DEFAULT, $ord = self::SEQ)
    {
        if (1 > $page) {
            throw new \RuntimeException("不支持的[page]参数。");
        }

        if (0 > $pageSize) {
            throw new \RuntimeException("不支持的[pageSize]参数。");
        }

        $key = self::$fp_prefix . ($env ?: self::DEFAULT) . ':' . ($uid ? self::$user_prefix . $uid : '');

        $start = ($page - 1) * $pageSize;
        $end = $page * $pageSize - 1;

        switch ($ord) {
            case self::HEAT:
                $method = "zRevRange";
                break;
            case self::SEQ:
                $method = "lRange";
                break;
            default:
                throw new \RuntimeException("不支持的排序参数。");
        }

        return $this->redis_obj->{$method}($key, $start, $end);
    }

    /**
     * 清除历史
     * @param null $uid 用户标识
     * @param null $env 场景
     * @return mixed
     */
    public function clear($uid = null, $env = self::DEFAULT)
    {
        $key = self::$fp_prefix . ($env ?: self::DEFAULT) . ':' . ($uid ? self::$user_prefix . $uid : '');

        return $this->redis_obj->del($key);
    }
}