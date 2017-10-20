`HeapKeyedStateBackend`��snapshot������Ҫ�������л��Լ�I/O�����������ܴ�ʱsnapshot�����Ĵ���ʱ�䣬���ʹ��ͬ�����ƽ���snapshot�����������ݴ������������������޷����̵ģ������Ҫһ���첽�Ľ�����������첽�����У�snapshot�����ݴ�����ִ�У�snapshotֻ��state���ж������������ݴ����state���ж�Ҳ��д�������Ҫ���state���������⡣Flink����ʵ����һ���첽snapshot�����������һ��֧��copy-on-write��incremental rehash��hash map��ǰ�߽���������⣬���߽��rehash���̿��ܵ��µ����ݴ����������⣬���ķֱ�����ֻ��ƽ���˵����

### Copy-on-write
`HeapKeyedStateBackend`ʹ��hash map�洢state��Flink����JDK��`HashMap`ʵ����copy-on-write���ƣ��Ӷ������state���������⡣hash map��Flink�ж�Ӧ����Ϊ`CopyOnWriteStateTable`��hash map��entry��Ӧ`StateTableEntry`������������copy-on-write��ص����ݳ�Ա������ʾ
```java
class CopyOnWriteStateTable<K, N, S> {

	// ��ǰmap�İ汾�ţ�ÿ����һ��snapshotֵ��1
	// ÿ��snapshot�İ汾�ż�snapshot����ʱ��stateTableVersion
	private int stateTableVersion;
	
	// δ���snapshot�İ汾�ż���
	// snapshot����ʱ����汾�ż��뼯��
	// snapshot�ͷ�ʱ����汾�ŴӼ����Ƴ�
	private TreeSet<Integer> snapshotVersions;

	// δ���snapshot����߰汾�ţ���snapshot�������ͷ�ʱ���޸�
	private int highestRequiredSnapshotVersion;
	
	// ���entry������
	private StateTableEntry<K, N, S>[] table;
}

class StateTableEntry<K, N, S> {
	// key��namespace����flink�еĸ�����߹�ͬ���map�е�key
	final K key;
    final N namespace;

	// ��Ӧmap�е�value
	S state;

	// ָ����һ��entry�������ͻ
	StateTableEntry<K, N, S> next;

	// entry�İ汾��
	int entryVersion;

	// state�İ汾��
	int stateVersion;
}
```

snapshot��Ϊͬ���������첽���֣�ͬ�����ֽ��������Ŀ�������������`CopyOnWriteStateTableSnapshot`�Ķ����첽ʹ�ã��첽����������л���I/O��������`CopyOnWriteStateTableSnapshot`����Ҫ���ݳ�ԱΪ
```java
class CopyOnWriteStateTableSnapshot<K, N, S> {

	// snapshot�İ汾��	
	private final int snapshotVersion;

	// ����snapshotʱ��map��table�Ŀ���
	private final StateTableEntry<K, N, S>[] snapshotData;
}
```

snapshot��������(ͬ��ִ��)��
* ��`map.table`������`snapshot.snapshotData`
* `++map.stateTableVersion`
* ����δ���snapshot����߰汾�ţ�`map.highestRequiredSnapshotVersion = map.stateTableVersion`������ӵ�δ���snapshot����`map.snapshotVersions`��
* snapshot�汾����Ϊ`map.stateTableVersion`

snapshot�ͷŹ���(�첽ִ��)��
* ��snapshot�汾��`snapshotVersion`��δ��ɼ���`map.snapshotVersions`���Ƴ�
* ����δ���snapshot����߰汾�ţ�`map.highestRequiredSnapshotVersion = map.snapshotVersions.last()`


copy-on-write������ԭ��
1. ��entry���޸�ʱ�����`entry.entryVersion < map.highestRequiredSnapshotVersion`��˵����entry���ڱ�ĳ��snapshotʹ�ã�Ӧ�ÿ���entry������entry��`entryVersion`����Ϊ`map.stateTableVersion`������map��һ��Ͱ�е�entry������ķ�ʽ������һ�������Ҫ�޸�ǰһ��entry��`next`ʹ��ָ�򿽱��õ�entry������ٴ��漰���޸�entry����Ҫ�ݹ��Ӧ��ԭ��1���������Ҫ������ֻ�轫`entryVersion`����Ϊ`map.stateTableVersion`����
2. ������`entry.state`���û�ʱ�����`entry.stateVersion < map.highestRequiredSnapshotVersion`��Ӧ�ý�state����deep copy�����Ű���ԭ��1����entry�����ԭ��1�п�����entry�����ÿ����õ���entryָ���µ�state������`entry.stateVersion`��Ϊ`map.stateTableVersion`��������ԭ����entryָ���µ�state������`entry.stateVersion`��Ϊ`map.stateTableVersion`


> ע�⣺�û��õ�state����ܽ����޸ģ������ʱĳ��snapshot����ʹ�����state���ͻ���־�������˼����ڽ���`map.get`����ʱҲҪ��state���п�������`key`��`namespace`û�н��п�������Ϊ����������ֻ��Flink�ڲ�ʹ�ã��û�����õ�


����ͨ��������˵��copy-on-write����

 _**��ʼ״̬**_

 * `stateTableVersion`��`hightestRequriedVersion`��ʼֵΪ`0`
 * `K`��ʾ`entry.key`��`entry.namespace`����ϣ�`S`��ʾ`entry.state`  
  ![1.png | center](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b1db87ed-147f-4608-8525-f6ee3535a12b.png "")


**_snapshot()_**

* ��`stateTableVersion`��`hightestRequriedVersion`����Ϊ`1`
* ��`map.table`������`snapshot.snapshotData`��snapshot�汾��Ϊ`1`
 ![2.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/c5c83e35-a153-4a14-9a2e-27e7ca4f8892.png) 


**_put(13, 2)_**

* map��û��`K:13`��ֱ�����entry������`stateVersion`��`entryVersion`��Ϊ`1`  
  ![3.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/5da5e1d2-842c-478c-88ac-6a4ca632465b.png) 

**_get(23)_**

* `stateVersion < highestRequiredVersion`��Ӧ�ù���2����`S:3`���п���
* Ӧ�ù���1����entry����Ϊ`entryVersion < highestRequiredVersion`��˵��entry���ڱ�snapshotʹ�ã���Ҫ���п���������`stateVersion`��`entryVersion`��Ϊ`1`��`K`ָ��ԭ����ֵ��`S`ָ�򿽱��õ���ֵ
* ��Ҫ�޸�ǰһ��entry`K:42`��`next`���ٴ�Ӧ�ù���1������entry��`stateVersion`������ȻΪ`0`��`entryVersion`��Ϊ`1`��`K`��`S`��ָ��ԭ����ֵ��`next`ָ����һ���еõ���entry
* tableָ���µ�entry`K:42`
 ![4.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/d3bf5fbc-99d6-43ca-b4e9-bb3fb956e2bd.png) 


**_get(42)_**

* `stateVersion < highestRequiredVersion`��Ӧ�ù���2����`S:7`���п���
* Ӧ�ù���1����entry����Ϊ`entryVersion == highestRequiredVersion`��˵��û��snapshot��ʹ��entry����˲���Ҫ������ֻ�轫`S`ָ���µ�state��������`stateVersion`Ϊ1  
  ![5.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/115b108c-565a-4357-be7e-6aed95c11969.png) 

**_release(1)_**
�ͷŰ汾��Ϊ1��snapshot

* ���汾��`1`��`map.snapshotVersions`���Ƴ�������`highestRequriedVersion`Ϊ0����ʾĿǰû���κ�snapshot����ִ��
* snapshotȡ����`snapshotData`�����ã��ͷ�ռ�õĿռ�  
  ![6.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/8034565d-af65-45e4-8b25-6489529ae29f.png) 


**_snapshot()_**

* ��`stateTableVersion`��`hightestRequriedVersion`����Ϊ`2`
* ��`map.table`������`snapshot.snapshotData`��snapshot�汾��Ϊ`2`
![7.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/e3a215e7-7c40-4e21-9d1d-339f252be73f.png) 

**_remove(23)_**

* ��Ҫ��`K:23`ǰһ��entry`K:42`��`next`��Ϊ`null`��Ӧ�ù���1��`entryVersion < hightestRequriedVersion`�������Ҫ����������`entryVersion`��Ϊ2��`K`��`S`ָ��ԭ����ֵ
* ��tableָ���µ�entry`K:42`
 ![8.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/9c236ffa-6a04-47bf-ba69-ae78f04a7fb1.png) 

**_release(2)_**  
�ͷŰ汾��Ϊ2��snapshot
* ���汾��`2`��`map.snapshotVersions`���Ƴ�������`highestRequriedVersion`Ϊ0
* snapshotȡ����`snapshotData`�����ã��ͷ�ռ�õĿռ�  
![9.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b97f1ce1-de25-4eab-b131-25a648d2de3d.png) 


### Incremental rehash(����ʽrehash)
hash mapΪ�˱�����ײ�����������½�����Ԫ�ظ���������ֵ����������Ŀռ䣬����Ԫ��rehash�����map�е�Ԫ�غܶ࣬һ����rehash����Ԫ�ؽ������������Ĵ������̣��Ӷ�����һ��ʱ���������½�������ʽrehash����������ƽ̯����map��ÿ�β���(get��put��remove)�У����map����rehash����ô����ǰ��rehashһ��������Ԫ�ء�����ʽrehash������ÿ��map�����Ĵ��ۣ��������ϵͳ���ȶ��ԡ�

����ͨ������������rehash�Ĺ���

**_��ʼ״̬_**
*  `oldCap`:  old map������
*  `newCap`:  new map������(`oldCap`������)
*  `rehashIdx`: old map����һ��Ӧ�ñ�rehash��Ͱ����������ʼֵΪ`0`��ÿ��rehash������`1`
*  ÿ��rehashһ��Ͱ�����е�Ԫ��
![1.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/7eb8d8ea-1ec7-4ab8-8478-3dce0e99e381.png) 

**_get(0)_**

* ��old map��Ͱ0������Ԫ��rehash��`K:0`�Ƶ�new map�У�`rehashIdx`��`1`��Ϊ`1`
* �ж�`K:0`���ĸ�map�У������ǱȽ�`k % oldCap`��`rehashIdx`�Ĵ�С�����ǰ��С��˵����Ԫ���������һ����rehash����new map�У���������old map�У�����`K:0`Ҫ��new map�в���
* ��new map�в���`K:0`������`V:2`

![2.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/f142cc61-5d38-4cf5-9a46-55f4d0eba5b4.png) 

**_put(3, 5)_**

* ��old map��Ͱ1������Ԫ��rehash��`K:1`��`K:5`�Ƶ�new map�У�`rehashIdx`����Ϊ`2`
* `3 % 4 > rehashIdx`����old map�в���`K:3`
* old map�в�����`K:3`����Ҫ�����µ�Ԫ�أ�ͬ��Ӧ�ò��뵽old map��

![3.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/b43b8df5-b7b5-4e0f-899f-7201f618f18f.png) 

**_remove(1)_**
* ��old map��Ͱ2������Ԫ��rehash��`K:6`�Ƶ�new map�У�`rehashIdx`����Ϊ`3`
* `1 % 4 < rehashIdx`����new map�в���`K:1`
* �ҵ�`K:1`���Ƴ�
![4.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/d55c6ff7-03cb-4e64-8513-6d30cf728df6.png) 

**_get(5)_**
* ��old map��Ͱ3������Ԫ��rehash��`K:3`�Ƶ�new map�У�`rehashIdx`����Ϊ`4`����ʱ`rehashIdx == oldCap`��˵��rehash��ɣ�old map���Զ�����ͬʱrehashIdx����Ϊ`0`
* ��new map�в���`K:5`������`V:7`
![5.png](https://private-alipayobjects.alipay.com/alipay-rmsdeploy-image/skylark/png/5485ae22-716e-48ae-8a80-97bb13a1a565.png) 


### �ο�����
[1] [[FLINK-5715]Asynchronous snapshotting for HeapKeyedStateBackend](https://issues.apache.org/jira/browse/FLINK-5715)  
[2] [A look at Flink��s internal data structures & algorithms for efficient checkpointing ](https://www.youtube.com/watch?v=dWQ24wERItM)  
[3] [redis��rehashʵ��](https://github.com/antirez/redis/blob/4.0.2/src/dict.c)

