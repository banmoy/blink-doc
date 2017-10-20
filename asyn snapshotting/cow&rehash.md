`HeapKeyedStateBackend`的snapshot过程主要包括序列化以及I/O，当数据量很大时snapshot将消耗大量时间，如果使用同步机制进行snapshot将会阻塞数据处理，这在生产环境是无法容忍的，因此需要一个异步的解决方案。在异步机制中，snapshot与数据处理并行执行，snapshot只对state进行读操作，而数据处理对state既有读也有写，因此需要解决state竞争的问题。Flink社区实现了一种异步snapshot方案，设计了一种支持copy-on-write和incremental rehash的hash map，前者解决竞争问题，后者解决rehash过程可能导致的数据处理阻塞问题，本文分别对两种机制进行说明。

### Copy-on-write
`HeapKeyedStateBackend`使用hash map存储state，Flink基于JDK的`HashMap`实现了copy-on-write机制，从而解决了state竞争的问题。hash map在Flink中对应的类为`CopyOnWriteStateTable`，hash map的entry对应`StateTableEntry`，两个类中与copy-on-write相关的数据成员如下所示
```java
class CopyOnWriteStateTable<K, N, S> {

	// 当前map的版本号，每创建一个snapshot值加1
	// 每个snapshot的版本号即snapshot创建时的stateTableVersion
	private int stateTableVersion;
	
	// 未完成snapshot的版本号集合
	// snapshot创建时将其版本号加入集合
	// snapshot释放时将其版本号从集合移除
	private TreeSet<Integer> snapshotVersions;

	// 未完成snapshot的最高版本号，在snapshot创建和释放时被修改
	private int highestRequiredSnapshotVersion;
	
	// 存放entry的数组
	private StateTableEntry<K, N, S>[] table;
}

class StateTableEntry<K, N, S> {
	// key和namespace都是flink中的概念，两者共同组成map中的key
	final K key;
    final N namespace;

	// 对应map中的value
	S state;

	// 指向下一个entry，解决冲突
	StateTableEntry<K, N, S> next;

	// entry的版本号
	int entryVersion;

	// state的版本号
	int stateVersion;
}
```

snapshot分为同步部分与异步部分，同步部分进行少量的拷贝，并创建类`CopyOnWriteStateTableSnapshot`的对象供异步使用，异步部分完成序列化和I/O操作，类`CopyOnWriteStateTableSnapshot`的主要数据成员为
```java
class CopyOnWriteStateTableSnapshot<K, N, S> {

	// snapshot的版本号	
	private final int snapshotVersion;

	// 创建snapshot时对map中table的拷贝
	private final StateTableEntry<K, N, S>[] snapshotData;
}
```

snapshot创建过程(同步执行)：
* 将`map.table`拷贝到`snapshot.snapshotData`
* `++map.stateTableVersion`
* 更新未完成snapshot的最高版本号，`map.highestRequiredSnapshotVersion = map.stateTableVersion`，并添加到未完成snapshot集合`map.snapshotVersions`中
* snapshot版本号设为`map.stateTableVersion`

snapshot释放过程(异步执行)：
* 将snapshot版本号`snapshotVersion`从未完成集合`map.snapshotVersions`中移除
* 更新未完成snapshot的最高版本号，`map.highestRequiredSnapshotVersion = map.snapshotVersions.last()`


copy-on-write的两个原则：
1. 当entry被修改时，如果`entry.entryVersion < map.highestRequiredSnapshotVersion`，说明该entry正在被某个snapshot使用，应该拷贝entry，并将entry的`entryVersion`设置为`map.stateTableVersion`，另外map将一个桶中的entry以链表的方式连接在一起，因此需要修改前一个entry的`next`使其指向拷贝得到entry，这就再次涉及到修改entry，需要递归地应用原则1；如果不需要拷贝则只需将`entryVersion`设置为`map.stateTableVersion`即可
2. 当返回`entry.state`给用户时，如果`entry.stateVersion < map.highestRequiredSnapshotVersion`，应该将state进行deep copy，接着按照原则1处理entry，如果原则1中拷贝了entry，则让拷贝得到的entry指向新的state，并将`entry.stateVersion`设为`map.stateTableVersion`，否则让原来的entry指向新的state，并将`entry.stateVersion`设为`map.stateTableVersion`


> 注意：用户得到state后可能进行修改，如果此时某个snapshot正在使用这个state，就会出现竞争，因此即便在进行`map.get`操作时也要将state进行拷贝，而`key`和`namespace`没有进行拷贝是因为这两个变量只在Flink内部使用，用户不会得到


下面通过例子来说明copy-on-write机制

 _**初始状态**_

 * `stateTableVersion`和`hightestRequriedVersion`初始值为`0`
 * `K`表示`entry.key`与`entry.namespace`的组合，`S`表示`entry.state`  
  ![1.png | center](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b1db87ed-147f-4608-8525-f6ee3535a12b.png "")


**_snapshot()_**

* 将`stateTableVersion`和`hightestRequriedVersion`更新为`1`
* 将`map.table`拷贝到`snapshot.snapshotData`，snapshot版本号为`1`
 ![2.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/c5c83e35-a153-4a14-9a2e-27e7ca4f8892.png) 


**_put(13, 2)_**

* map中没有`K:13`，直接添加entry，将其`stateVersion`和`entryVersion`设为`1`  
  ![3.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/5da5e1d2-842c-478c-88ac-6a4ca632465b.png) 

**_get(23)_**

* `stateVersion < highestRequiredVersion`，应用规则2，将`S:3`进行拷贝
* 应用规则1处理entry，因为`entryVersion < highestRequiredVersion`，说明entry正在被snapshot使用，需要进行拷贝，并将`stateVersion`和`entryVersion`设为`1`，`K`指向原来的值，`S`指向拷贝得到的值
* 需要修改前一个entry`K:42`的`next`，再次应用规则1，拷贝entry，`stateVersion`不变依然为`0`，`entryVersion`设为`1`，`K`和`S`均指向原来的值，`next`指向上一步中得到的entry
* table指向新的entry`K:42`
 ![4.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/d3bf5fbc-99d6-43ca-b4e9-bb3fb956e2bd.png) 


**_get(42)_**

* `stateVersion < highestRequiredVersion`，应用规则2，将`S:7`进行拷贝
* 应用规则1处理entry，因为`entryVersion == highestRequiredVersion`，说明没有snapshot在使用entry，因此不需要拷贝，只需将`S`指向新的state，并设置`stateVersion`为1  
  ![5.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/115b108c-565a-4357-be7e-6aed95c11969.png) 

**_release(1)_**
释放版本号为1的snapshot

* 将版本号`1`从`map.snapshotVersions`中移除，更新`highestRequriedVersion`为0，表示目前没有任何snapshot正在执行
* snapshot取消对`snapshotData`的引用，释放占用的空间  
  ![6.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/8034565d-af65-45e4-8b25-6489529ae29f.png) 


**_snapshot()_**

* 将`stateTableVersion`和`hightestRequriedVersion`更新为`2`
* 将`map.table`拷贝到`snapshot.snapshotData`，snapshot版本号为`2`
![7.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/e3a215e7-7c40-4e21-9d1d-339f252be73f.png) 

**_remove(23)_**

* 需要将`K:23`前一个entry`K:42`的`next`设为`null`，应用规则1，`entryVersion < hightestRequriedVersion`，因此需要拷贝，并将`entryVersion`设为2，`K`和`S`指向原来的值
* 将table指向新的entry`K:42`
 ![8.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/9c236ffa-6a04-47bf-ba69-ae78f04a7fb1.png) 

**_release(2)_**  
释放版本号为2的snapshot
* 将版本号`2`从`map.snapshotVersions`中移除，更新`highestRequriedVersion`为0
* snapshot取消对`snapshotData`的引用，释放占用的空间  
![9.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b97f1ce1-de25-4eab-b131-25a648d2de3d.png) 


### Incremental rehash(渐进式rehash)
hash map为了避免碰撞带来的性能下降，在元素个数超过阈值后会申请更大的空间，并将元素rehash。如果map中的元素很多，一次性rehash所有元素将会阻塞正常的处理流程，从而导致一段时间内性能下降。渐进式rehash将整个过程平摊到对map的每次操作(get，put，remove)中，如果map正在rehash，那么操作前先rehash一定数量的元素。渐进式rehash增加了每次map操作的代价，但提高了系统的稳定性。

下面通过例子来解释rehash的过程

**_初始状态_**
*  `oldCap`:  old map的容量
*  `newCap`:  new map的容量(`oldCap`的两倍)
*  `rehashIdx`: old map中下一个应该被rehash的桶的索引，初始值为`0`，每次rehash结束加`1`
*  每次rehash一个桶中所有的元素
![1.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/7eb8d8ea-1ec7-4ab8-8478-3dce0e99e381.png) 

**_get(0)_**

* 将old map中桶0的所有元素rehash，`K:0`移到new map中，`rehashIdx`加`1`变为`1`
* 判断`K:0`在哪个map中，方法是比较`k % oldCap`与`rehashIdx`的大小，如果前者小则说明该元素如果存在一定被rehash到了new map中，否则仍在old map中，这里`K:0`要在new map中查找
* 在new map中查找`K:0`并返回`V:2`

![2.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/f142cc61-5d38-4cf5-9a46-55f4d0eba5b4.png) 

**_put(3, 5)_**

* 将old map中桶1的所有元素rehash，`K:1`和`K:5`移到new map中，`rehashIdx`更新为`2`
* `3 % 4 > rehashIdx`，在old map中查找`K:3`
* old map中不存在`K:3`，需要插入新的元素，同样应该插入到old map中

![3.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b43b8df5-b7b5-4e0f-899f-7201f618f18f.png) 

**_remove(1)_**
* 将old map中桶2的所有元素rehash，`K:6`移到new map中，`rehashIdx`更新为`3`
* `1 % 4 < rehashIdx`，在new map中查找`K:1`
* 找到`K:1`并移除
![4.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/d55c6ff7-03cb-4e64-8513-6d30cf728df6.png) 

**_get(5)_**
* 将old map中桶3的所有元素rehash，`K:3`移到new map中，`rehashIdx`更新为`4`，此时`rehashIdx == oldCap`，说明rehash完成，old map可以丢弃，同时rehashIdx重置为`0`
* 在new map中查找`K:5`并返回`V:7`
![5.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/5485ae22-716e-48ae-8a80-97bb13a1a565.png) 


### 参考资料
[1] [[FLINK-5715]Asynchronous snapshotting for HeapKeyedStateBackend](https://issues.apache.org/jira/browse/FLINK-5715)  
[2] [A look at Flink’s internal data structures & algorithms for efficient checkpointing ](https://www.youtube.com/watch?v=dWQ24wERItM)  
[3] [redis的rehash实现](https://github.com/antirez/redis/blob/4.0.2/src/dict.c)

