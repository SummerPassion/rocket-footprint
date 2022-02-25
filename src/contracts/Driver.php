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
 * DateTime: 2022-02-25 19:11:44
 */

namespace rocket\contracts;

/**
 * Class Driver
 * @package rocket\contracts
 * create_at: 2022-02-25 19:12:33
 * update_at: 2022-02-25 19:12:33
 */
abstract class Driver
{
    /**
     * 热度
     */
    const HEAT = 'HEAT';

    /**
     * 时序
     */
    const SEQ = 'SEQ';

    /**
     * 默认存储
     */
    const DEFAULT = 'Default';

    /**
     * 起始页
     */
    const PAGE = 1;

    /**
     * 每页数据量
     */
    const PAGESIZE = 10;

    /**
     * 前缀
     * @var string
     */
    public static $fp_prefix = 'Fp_';

    /**
     * 用户前缀
     * @var string
     */
    public static $user_prefix = 'Uid_';

    /**
     * 默认最大字符
     * @var int
     */
    protected $max_len = 50;

    /**
     * 记录
     * @return mixed
     */
    public abstract function log($val, $uid = null, $ord = null, $env = null);

    /**
     * 获取
     * @return mixed
     */
    public abstract function get($end, $uid = null, $ord = self::SEQ, $env = self::DEFAULT);

    /**
     * 清空
     * @return mixed
     */
    public abstract function clear($uid = null, $env = null);
}