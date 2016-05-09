package fourquant;

import ij.ImagePlus;
import ij.plugin.filter.PlugInFilter;
import ij.process.ImageProcessor;

import java.io.Serializable;

/**
 * A fake imagej plugin for testing purposes
 * Created by mader on 5/9/16.
 */
public class FakePlugin implements PlugInFilter, Serializable {

    @Override
    public int setup(String s, ImagePlus imagePlus) {
        return 0;
    }

    @Override
    public void run(ImageProcessor imageProcessor) {
        System.out.println("FakePlugin has run!");
    }
}
