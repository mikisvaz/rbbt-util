if __name__ == "__main__":
    import rbbt
    import rbbt.workflow
    wf = rbbt.workflow.Workflow('Baking')
    step = wf.fork('bake_muffin_tray', add_blueberries=True, clean='recursive')
    step.join()
    print(step.load())



