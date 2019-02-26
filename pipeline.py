import luigi

from tasks.features.merge import MergeFeatures


if __name__ == '__main__':
    luigi.run(main_task_cls=MergeFeatures, local_scheduler=True)
