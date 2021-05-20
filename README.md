# decision-tree-spark

## 版本信息：

@version 2021/3/18 V1.0.0

@version 2021/3/29 V1.0.1 对数据源抽象部分进行了改动，由实现Source类变更为在Json配置文件中配置

@version 2021/3/29 V1.0.2 XGBoot模型在计算时，需要传入一个特征向量，需要使用Array<Double>类型。经过测试，发现这种复杂类型在数据传输过程中需要占用较大的内存，影响计算效率。解决方案：在向量维度较多时，我们提供将向量转换成稀疏数组Array<Array<Double>>，再将其转为字符串格式，这样在数据传递过程中就能大幅减少内存占用。

@version 2021/3/29 V1.0.3 增加了节点对PMML的支持，增加Model实现类PmmlModel

@version 2021/3/29 V1.0.4 增加source字段，用于区分特征计算的数据来源

@version 2021/3/29 V1.0.5 对[[com.yang.DecisionTreeApplication]]入口类进行封装，提供外部开发测试的上下文[[com.yang.decision.ApplicationContext]]，示例：

```scala
def main(args: Array[String]): Unit = {
    val conf = Seq(
      (Configuration.PROCESS_TIME, String.valueOf(1)),
      (Configuration.IS_LOCAL, String.valueOf(true)),
      (Configuration.IS_CACHE, String.valueOf(false)),
      (Configuration.ENABLE_SPARSE_VECTOR, String.valueOf(false)),
      ("local.source", "car.csv")
    )
    val context = DecisionTreeApplication
      .builder
      .config(conf)
      .args(args)
      .create()
    context.run()
}
```

## 项目背景：

在集团数据挖掘项目下，用户画像工程需要开发很多基础标签，如年龄、性别、婚姻状况、资产情况等标签。这些标签不能使用集团下的各个数据源来简单的处理后，得到结果作为最终评价结果，因为不同数据源的数据质量参差不齐，且各个数据源得到的结果置信度情况也不一样。因此我们需要使用决策树+XGBoost等模型来综合预测用户标签。

## 案例：年龄段

### 数据源：金融实名信息、信安实名信息、简历出生年月、umc用户中心信息

### 特征结构：(金融年龄，信安年龄，简历年龄，UMC年龄，简历年龄?=UMC年龄，appList)

一开始，我准备参照机器学习的PMML来实现决策树的抽象功能，但是看了一整天的PMML官方文档和一大堆PMML相关的文章后，我发现这种方案对我来实现起来太过复杂。机器学习的PMML文件大多都是模型代码自动生成的，不适合我们这种场景（我们是要自己实现把决策树转换为PMML文件）。如果想要我们来写生成PMML文件的代码，那么需要精通PMML的各类标签定义，以及在spark环境下怎么加载该文件，以及如何保证加载的Model是我们想要的逻辑，这里实现起来，后续工作可能很复杂。

没办法，我只能思考在我们自己的需求背景下，怎么自己写代码来实现决策树模型的抽象功能。

它至少包括以下几个难点：
- 一、如何抽象决策树
- 二、如何设计类似PMML的配置文件
- 三、如何解析配置文件
- 四、如何转换成spark可执行的代码

### 解决思路：PMML、二叉树、前序遍历、深度优先、分支策略

### 配置文件示例：

```json
{
    "schema": "id, jr_age, xinan_age, jl_age, umc_age, yc_age, vector",
    "source": "xxxxxx, age_group",
    "sink": "xxxxxx, age_group",
    "rules": {
        "rule": "jr_age != null",
        "left": {
            "rule": "GeneralRule, jr_age, 1.0"
        },
        "right": {
            "rule": "xinan_age != null",
            "left": {
                "rule": "GeneralRule, xinan_age, 0.997349184"
            },
            "right": {
                "rule": "jl_age != null || umc_age != null || yc_age != null",
                "left": {
                    "rule": "jl_age == umc_age || jl_age == yc_age || umc_age == yc_age",
                    "left": {
                        "rule": "jl_age == umc_age || jl_age == yc_age",
                        "left": {
                            "rule": "GeneralRule, jl_age, 0.992448605"
                        },
                        "right": {
                            "rule": "GeneralRule, umc_age, 0.992448605"
                        }
                    },
                    "right": {
                        "rule": "jl_age != null",
                        "left": {
                            "rule": "GeneralRule, jl_age, 0.982582546"
                        },
                        "right": {
                            "rule": "umc_age != null",
                            "left": {
                                "rule": "GeneralRule, umc_age, 0.974128879"
                            },
                            "right": {
                                "rule": "GeneralRule, yc_age, 0.920175899"
                            }
                        }
                    }
                },
                "right": {
                    "rule": "vector != null",
                    "left": {
                        "rule": "ModelRule, XGBoost, /xxxxxx/xxxxxx/xxxxxx/age-applist-xgb-0901, null"
                    }
                }
            }
        }
    }
}
```

## 模型抽象：

```
分支节点和叶子节点使用：策略模式 Strategy

ModelProperties：元数据配置
    schema: Array[String] 数据源字段信息
    input: Array[String] 数据源表信息
    output: Array[String] 输出表信息

OutputSchema：输出数据集，叶子节点的值
    id：String 主键
    featureValue：String 特征值
    confidence：Double 置信度

Rule：计算规则
    execute(seq：DataFrame):DataFrame 执行计算

GeneralRule(rule: String)：一般计算，继承自Rule

ModelRule(modelClass: String,
            modelFilePath: String,
            otherConf： Map[String,String])：模型计算，继承自Rule

Calculation：计算节点（叶子节点），继承自Leaf
    seq：DataFrame 输入特征数据
    rule：Rule 计算规则

Condition：条件节点（分支节点），继承自Branch
    seq：DataFrame 输入特征数据
    //rule：Rule 计算规则
    condition：条件表达式

Parser：解析器，将输出text解析为一棵二叉树，并实现绑定规则的功能
    parse：Tree 解析功能

Binder：绑定器，根据解析后的二叉树和节点规则，将输入数据依次绑定到各个节点
    bind：Tree 绑定功能

Executor：执行器，遍历整棵树，将需要计算的节点数据merge起来输出
    execute：Unit 执行计算
    executeAndSaveResultData: Unit 执行计算并写出数据
```
