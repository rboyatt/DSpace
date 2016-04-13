/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.mediafilter;

import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.Dimension;
import java.awt.RenderingHints;
import java.awt.Graphics2D;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import java.util.List;

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSlide;

/**
 * Filter powerpoint bitstreams, scaling the first slide image to be within the bounds of
 * thumbnail.maxwidth, thumbnail.maxheight, the size we want our thumbnail to be
 * no bigger than. Creates only JPEGs.
 *
 * @author Russell Boyatt <russell.boyatt@warwick.ac.uk>
 */
public class PowerPointThumbFilter extends JPEGFilter implements SelfRegisterInputFormats
{
    /**
     * @param source
     *            source input stream
     * 
     * @return InputStream the resulting input stream
     */
    @Override
    public InputStream getDestinationStream(Item currentItem, InputStream source, boolean verbose)
            throws Exception
    {
        // Read the powerpoint file
        XMLSlideShow ppt = new XMLSlideShow(source);

        //getting the dimensions and size of the slide
        Dimension pgsize = ppt.getPageSize();
        List<XSLFSlide> slide = ppt.getSlides();

        // read in bitstream's image
        BufferedImage buf = new BufferedImage(pgsize.width, pgsize.height,BufferedImage.TYPE_INT_RGB);
        Graphics2D graphicsoriginal = buf.createGraphics();
        if(slide.size() > 0) {
            slide.get(0).draw(graphicsoriginal);
        }

        // get config params
        float xmax = (float) ConfigurationManager.getIntProperty("thumbnail.maxwidth");
        float ymax = (float) ConfigurationManager.getIntProperty("thumbnail.maxheight");
        boolean blurring = (boolean) ConfigurationManager.getBooleanProperty("thumbnail.blurring");
        boolean hqscaling = (boolean) ConfigurationManager.getBooleanProperty("thumbnail.hqscaling");

        // now get the image dimensions
        float xsize = (float) pgsize.width;
        float ysize = (float) pgsize.height;

        // if verbose flag is set, print out dimensions
        // to STDOUT
        if (verbose)
        {
            System.out.println("original size: " + xsize + "," + ysize);
        }

        // scale by x first if needed
        if (xsize > xmax)
        {
            float scale_factor = xmax / xsize;

            if (verbose)
            {
                System.out.println("x scale factor: " + scale_factor);
            }

            xsize = xsize * scale_factor;
            ysize = ysize * scale_factor;

            if (verbose)
            {
                System.out.println("new size: " + xsize + "," + ysize);
            }
        }

        // scale by y if needed
        if (ysize > ymax)
        {
            float scale_factor = ymax / ysize;

            // now reduce x size
            // and y size
            xsize = xsize * scale_factor;
            ysize = ysize * scale_factor;
        }

        // if verbose flag is set, print details to STDOUT
        if (verbose)
        {
            System.out.println("created thumbnail size: " + xsize + ", "
                    + ysize);
        }

        // create an image buffer for the thumbnail with the new xsize, ysize
        BufferedImage thumbnail = new BufferedImage((int) xsize, (int) ysize,
                BufferedImage.TYPE_INT_RGB);

        // Use blurring if selected in config.
        // a little blur before scaling does wonders for keeping moire in check.
        if (blurring)
        {
                // send the buffered image off to get blurred.
                buf = getBlurredInstance((BufferedImage) buf);
        }

        // Use high quality scaling method if selected in config.
        // this has a definite performance penalty.
        if (hqscaling)
        {
                // send the buffered image off to get an HQ downscale.
                buf = getScaledInstance((BufferedImage) buf, (int) xsize, (int) ysize,
                        (Object) RenderingHints.VALUE_INTERPOLATION_BICUBIC, (boolean) true);
        }

        // now render the image into the thumbnail buffer
        Graphics2D g2d = thumbnail.createGraphics();
        g2d.drawImage(buf, 0, 0, (int) xsize, (int) ysize, null);

        // now create an input stream for the thumbnail buffer and return it
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ImageIO.write(thumbnail, "jpeg", baos);

        // now get the array
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        return bais; // hope this gets written out before its garbage collected!
    }


    @Override
    public String[] getInputMIMETypes()
    {
        String[] types = {"application/vnd.openxmlformats-officedocument.presentationml.presentation"};
        return types;
    }

    @Override
    public String[] getInputDescriptions()
    {
        return null;
    }

    @Override
    public String[] getInputExtensions()
    {
        // Temporarily disabled as JDK 1.6 only
        // return ImageIO.getReaderFileSuffixes();
        return null;
    }

}
